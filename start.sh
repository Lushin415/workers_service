#!/bin/bash

# Workers Service Startup Script

echo "=========================================="
echo "  WORKERS SERVICE"
echo "=========================================="
echo ""
echo "Запуск сервиса на порту 8002..."
echo ""
echo "Swagger UI: http://localhost:8002/docs"
echo "ReDoc: http://localhost:8002/redoc"
echo ""
echo "Нажмите Ctrl+C для остановки"
echo "=========================================="
echo ""

python api.py
