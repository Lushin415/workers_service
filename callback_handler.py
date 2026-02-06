"""
Обработчик callback-кнопок Telegram (polling)

Обрабатывает нажатия кнопок:
- check_blacklist:{item_id} — проверка найденного объявления в ЧС
- ignore:{item_id} — игнорировать объявление
"""
import asyncio
import threading
from typing import Optional
from loguru import logger

from telegram import Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from db_service import DBService
from blacklist_service import BlacklistService


class CallbackHandler:
    """Обработчик callback-кнопок Telegram"""

    def __init__(
        self,
        bot_token: str,
        blacklist_service: BlacklistService,
        db_service: DBService
    ):
        self.bot_token = bot_token
        self.blacklist_service = blacklist_service
        self.db = db_service
        self.application: Optional[Application] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def _setup_handlers(self):
        """Настроить обработчики callback-кнопок"""
        # Проверка в ЧС (из уведомления)
        self.application.add_handler(
            CallbackQueryHandler(
                self._handle_blacklist_check,
                pattern=r"^check_blacklist:\d+$"
            )
        )

        # Игнорировать объявление
        self.application.add_handler(
            CallbackQueryHandler(
                self._handle_ignore,
                pattern=r"^ignore:\d+$"
            )
        )

        logger.info("CallbackHandler: обработчики настроены")

    async def _handle_blacklist_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработка кнопки 'Проверить в ЧС'

        callback_data: check_blacklist:{item_id}
        """
        query = update.callback_query
        await query.answer("Поиск в черном списке...")

        try:
            # Извлекаем item_id из callback_data
            callback_data = query.data
            item_id = int(callback_data.split(":")[1])

            logger.info(f"Проверка в ЧС для объявления ID: {item_id}")

            # Получаем данные объявления из БД
            item = await self.db.get_found_item_by_id(item_id)

            if not item:
                await query.message.reply_text("❌ Объявление не найдено в базе данных")
                return

            # Получаем author_id и author_username
            author_id = item.author_id
            author_username = item.author_username

            if not author_id and not author_username:
                await query.edit_message_reply_markup(reply_markup=None)
                await query.message.reply_text(
                    "❌ Не удалось проверить в ЧС\n\n"
                    "Telegram User ID и username автора неизвестны.\n"
                    "Возможно, сообщение было отправлено анонимно."
                )
                return

            # Получаем blacklist_session_path из задачи (для этого пользователя)
            bl_session = await self.db.get_blacklist_session_by_item(item_id)

            # Отправляем сообщение о начале поиска
            search_msg = await query.message.reply_text(
                "🔍 Поиск в черном списке...\n"
                f"User ID: {author_id or 'неизвестен'}\n"
                f"Username: {author_username or 'неизвестен'}"
            )

            # Ищем в черном списке (в реальном времени, с сессией пользователя)
            result = await self.blacklist_service.search_in_blacklist(
                user_id=author_id,
                username=author_username,
                session_name=bl_session
            )

            # Удаляем сообщение о поиске
            await search_msg.delete()

            # Формируем и отправляем результат
            if result.get("found"):
                response = self._format_blacklist_found(result)
            else:
                response = self._format_blacklist_not_found(result, author_id, author_username)

            # Убираем кнопки с оригинального сообщения
            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text(response, disable_web_page_preview=False)

        except Exception as e:
            logger.error(f"Ошибка обработки check_blacklist: {e}")
            await query.message.reply_text(f"❌ Ошибка проверки: {e}")

    async def _handle_ignore(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработка кнопки 'Игнорировать'

        callback_data: ignore:{item_id}
        """
        query = update.callback_query
        await query.answer("Объявление проигнорировано")

        try:
            # Просто убираем кнопки
            await query.edit_message_reply_markup(reply_markup=None)

        except Exception as e:
            logger.error(f"Ошибка обработки ignore: {e}")

    def _format_blacklist_found(self, result: dict) -> str:
        """Форматировать сообщение 'Найден в ЧС'"""
        parts = [
            "⚠️ НАЙДЕН В ЧЕРНОМ СПИСКЕ!",
            ""
        ]

        # В каком чате найден
        if result.get("chat"):
            parts.append(f"💬 Чат: {result['chat']}")

        extracted = result.get("extracted_info", {})

        if extracted.get("user_id"):
            parts.append(f"👤 ID: {extracted['user_id']}")

        if extracted.get("role"):
            role_ru = "Работодатель" if extracted["role"] == "employer" else "Сотрудник"
            parts.append(f"📋 Роль: {role_ru}")

        if extracted.get("full_name"):
            parts.append(f"📝 ФИО: {extracted['full_name']}")

        if extracted.get("username"):
            parts.append(f"🔗 Ник: {extracted['username']}")

        if extracted.get("phone"):
            parts.append(f"📞 Тел: {extracted['phone']}")

        parts.append("")
        parts.append("🔗 Сообщение в ЧС:")
        parts.append(result.get("message_link", ""))

        return "\n".join(parts)

    def _format_blacklist_not_found(self, result: dict, author_id: int, author_username: str) -> str:
        """Форматировать сообщение 'Не найден в ЧС'"""
        parts = [
            "✅ В черном списке НЕ найден",
            ""
        ]

        if author_id:
            parts.append(f"User ID: {author_id}")
        if author_username:
            parts.append(f"Username: @{author_username.lstrip('@')}")

        parts.append("")
        parts.append(f"Проверено сообщений: {result.get('messages_checked', 0)}")

        # Показываем в каких чатах искали
        chats_checked = result.get("chats_checked", [])
        if chats_checked:
            parts.append(f"Проверено чатов: {len(chats_checked)}")

        if result.get("error"):
            parts.append(f"\n⚠️ Ошибка: {result['error']}")

        return "\n".join(parts)

    async def _run_polling_async(self):
        """Асинхронный polling без сигналов (для фонового потока)"""
        try:
            # Создаём Application
            self.application = Application.builder().token(self.bot_token).build()

            # Настраиваем обработчики
            self._setup_handlers()

            # Инициализируем application
            await self.application.initialize()
            await self.application.start()

            # Запускаем polling вручную (без signal handlers)
            logger.info("CallbackHandler: запуск polling...")
            await self.application.updater.start_polling(
                allowed_updates=["callback_query"],
                drop_pending_updates=True
            )

            # Ждём пока _running == True
            while self._running:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"CallbackHandler: ошибка polling: {e}")
        finally:
            # Корректная остановка
            try:
                if self.application:
                    if self.application.updater.running:
                        await self.application.updater.stop()
                    await self.application.stop()
                    await self.application.shutdown()
            except Exception as e:
                logger.error(f"CallbackHandler: ошибка при остановке: {e}")

    def _run_polling(self):
        """Запустить polling в отдельном event loop"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self._run_polling_async())
        except Exception as e:
            logger.error(f"CallbackHandler: ошибка в event loop: {e}")
        finally:
            loop.close()

    def start_polling(self):
        """Запустить polling в фоновом потоке"""
        if self._running:
            logger.warning("CallbackHandler: polling уже запущен")
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_polling, daemon=True)
        self._thread.start()
        logger.info("CallbackHandler: polling запущен в фоновом потоке")

    def stop_polling(self):
        """Остановить polling"""
        if not self._running:
            return

        # Устанавливаем флаг остановки
        self._running = False
        logger.info("CallbackHandler: отправлен сигнал остановки polling")
