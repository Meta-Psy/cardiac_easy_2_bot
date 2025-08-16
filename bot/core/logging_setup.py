"""
Настройка логирования для бота
"""
import logging
import sys

def setup_logging():
    """Настройка логирования без эмодзи для совместимости с Windows"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    file_handler = logging.FileHandler('bot.log', encoding='utf-8')
    console_handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(console_handler)
    
    try:
        # Настройка для файла с UTF-8
        file_handler = logging.FileHandler('bot.log', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(log_format))
        
        # Настройка для консоли с безопасной кодировкой
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(log_format))
        
        # Для Windows устанавливаем безопасную кодировку
        if sys.platform.startswith('win'):
            import locale
            import codecs
            
            # Попытка установить UTF-8 кодировку для Windows
            try:
                # Для Python 3.7+
                if hasattr(sys.stdout, 'reconfigure'):
                    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
                    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
                else:
                    # Для старых версий Python
                    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'replace')
                    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'replace')
            except:
                # Если ничего не помогает, используем безопасную кодировку
                pass
    except Exception as e:
        # Fallback для проблемных систем
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler('bot.log', encoding='utf-8', errors='replace'),
                logging.StreamHandler()
            ]
        )