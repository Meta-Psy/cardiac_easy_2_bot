"""
Модели базы данных SQLAlchemy
"""
from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Text,
    Boolean,
    ForeignKey,
    Index,
    BigInteger,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class User(Base):
    """Модель пользователя с контактными данными"""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    phone = Column(String(50), nullable=True)

    # Статусы прохождения
    completed_diagnostic = Column(Boolean, default=False, nullable=False)
    registration_completed = Column(Boolean, default=False, nullable=False)
    survey_completed = Column(Boolean, default=False, nullable=False)
    tests_completed = Column(Boolean, default=False, nullable=False)
    
    # Статусы для ТОЧКА 2 (рассылка после вебинара)
    webinar_watched = Column(Boolean, default=False, nullable=False)  # Просмотрел ли вебинар
    webinar_survey_completed = Column(Boolean, default=False, nullable=False)  # Прошел ли опрос ТОЧКА 2
    bonus_received = Column(Boolean, default=False, nullable=False)  # Получил ли бонус

    # Временные метки
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )
    last_activity = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Связи с другими таблицами
    surveys = relationship(
        "Survey", back_populates="user", cascade="all, delete-orphan"
    )
    test_results = relationship(
        "TestResult", back_populates="user", cascade="all, delete-orphan"
    )
    activity_logs = relationship(
        "ActivityLog", back_populates="user", cascade="all, delete-orphan"
    )
    webinar_status = relationship(
        "WebinarStatus", back_populates="user", cascade="all, delete-orphan"
    )
    webinar_surveys = relationship(
        "WebinarSurvey", back_populates="user", cascade="all, delete-orphan"
    )
    followup_surveys = relationship(
        "FollowUpSurvey", back_populates="user", cascade="all, delete-orphan"
    )
    followup_status = relationship(
        "FollowUpStatus", back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<User(telegram_id={self.telegram_id}, name='{self.name}')>"


class Survey(Base):
    """Модель опроса о здоровье (18 вопросов)"""

    __tablename__ = "surveys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(
        BigInteger, ForeignKey("users.telegram_id"), nullable=False, index=True
    )

    # Демографические данные (вопросы 1-7)
    age = Column(Integer, nullable=True)
    gender = Column(String(50), nullable=True)
    location = Column(String(255), nullable=True)
    education = Column(String(255), nullable=True)
    family_status = Column(String(255), nullable=True)
    children = Column(String(255), nullable=True)
    income = Column(String(255), nullable=True)

    # Здоровье и осведомленность (вопросы 8-14)
    health_rating = Column(Integer, nullable=True)  # 0-10
    death_cause = Column(String(255), nullable=True)
    heart_disease = Column(String(255), nullable=True)
    cv_risk = Column(String(255), nullable=True)
    cv_knowledge = Column(String(255), nullable=True)
    heart_danger = Column(Text, nullable=True)  # JSON список (до 3 вариантов)
    health_importance = Column(String(255), nullable=True)
    
    # Профилактика и проверки здоровья
    checkup_history = Column(String(255), nullable=True)
    checkup_content = Column(Text, nullable=True)  # JSON список
    
    # Барьеры профилактики
    prevention_barriers = Column(Text, nullable=True)  # JSON список
    prevention_barriers_other = Column(Text, nullable=True)  # Поле "Другое"
    health_advice = Column(Text, nullable=True)  # JSON список (до 2 вариантов)
    
    lifestyle_factors = Column(Text, nullable=True)  # JSON строка
    diet = Column(String(255), nullable=True)
    diet_score = Column(Integer, nullable=True)  # 0-10

    # Медицинская грамотность (вопросы 15-18)
    med_literacy_1 = Column(Integer, nullable=True)  # 1-5
    med_literacy_2 = Column(Integer, nullable=True)  # 1-5
    med_literacy_3 = Column(Integer, nullable=True)  # 1-5
    med_literacy_4 = Column(Integer, nullable=True)  # 1-5

    # Временные метки
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Связь с пользователем
    user = relationship("User", back_populates="surveys")

    def __repr__(self):
        return f"<Survey(telegram_id={self.telegram_id}, completed_at={self.completed_at})>"


class TestResult(Base):
    """Модель результатов психологических тестов"""

    __tablename__ = "test_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(
        BigInteger, ForeignKey("users.telegram_id"), nullable=False, index=True
    )

    # Результаты тестов (баллы)
    hads_anxiety_score = Column(Integer, nullable=True)
    hads_depression_score = Column(Integer, nullable=True)
    hads_total_score = Column(Integer, nullable=True)
    hads_anxiety_level = Column(String(50), nullable=True)  # норма/субклин/клин
    hads_depression_level = Column(String(50), nullable=True)
    
    burns_score = Column(Integer, nullable=True)
    burns_level = Column(String(50), nullable=True)  # минимальная/легкая/умеренная/тяжелая/крайне тяжелая
    
    isi_score = Column(Integer, nullable=True)
    isi_level = Column(String(50), nullable=True)  # нет/подпороговая/умеренная/тяжелая
    
    stop_bang_score = Column(Integer, nullable=True)
    stop_bang_risk = Column(String(50), nullable=True)  # низкий/умеренный/высокий
    
    ess_score = Column(Integer, nullable=True)
    ess_level = Column(String(50), nullable=True)  # норма/легкая/умеренная/выраженная
    
    fagerstrom_score = Column(Integer, nullable=True)
    fagerstrom_level = Column(String(50), nullable=True)  # очень низкая/низкая/низкая-умеренная/умеренная/высокая
    
    audit_score = Column(Integer, nullable=True)
    audit_level = Column(String(50), nullable=True)  # зона 1-4

    # Флаги пропуска тестов
    fagerstrom_skipped = Column(Boolean, default=False)
    audit_skipped = Column(Boolean, default=False)

    # Детальные ответы (JSON строки)
    hads_answers = Column(Text, nullable=True)
    burns_answers = Column(Text, nullable=True)
    isi_answers = Column(Text, nullable=True)
    stop_bang_answers = Column(Text, nullable=True)
    ess_answers = Column(Text, nullable=True)
    fagerstrom_answers = Column(Text, nullable=True)
    audit_answers = Column(Text, nullable=True)

    # Интерпретации результатов
    hads_interpretation = Column(Text, nullable=True)
    burns_interpretation = Column(Text, nullable=True)
    isi_interpretation = Column(Text, nullable=True)
    stop_bang_interpretation = Column(Text, nullable=True)
    ess_interpretation = Column(Text, nullable=True)
    fagerstrom_interpretation = Column(Text, nullable=True)
    audit_interpretation = Column(Text, nullable=True)

    # Комплексная оценка рисков
    risk_assessment = Column(Text, nullable=True)  # JSON строка
    overall_risk_level = Column(String(50), nullable=True)
    overall_risk_score = Column(Integer, nullable=True)
    overall_cv_risk_level = Column(String(50), nullable=True)  # НИЗКИЙ/УМЕРЕННЫЙ/ВЫСОКИЙ/ОЧЕНЬ ВЫСОКИЙ
    overall_cv_risk_score = Column(Integer, nullable=True)
    risk_factors_count = Column(Integer, nullable=True)
    personalized_recommendations = Column(Text, nullable=True)

    # Временные метки
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Связь с пользователем
    user = relationship("User", back_populates="test_results")

    def __repr__(self):
        return f"<TestResult(telegram_id={self.telegram_id}, completed_at={self.completed_at})>"


class ActivityLog(Base):
    """Лог активности пользователей для отладки и аналитики"""

    __tablename__ = "activity_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(
        BigInteger, ForeignKey("users.telegram_id"), nullable=False, index=True
    )

    # Информация о действии
    action = Column(String(255), nullable=False, index=True)
    details = Column(Text, nullable=True)  # JSON строка с деталями
    state = Column(String(255), nullable=True)  # FSM состояние

    # Данные сообщения
    message_id = Column(Integer, nullable=True)
    message_text = Column(Text, nullable=True)
    callback_data = Column(String(255), nullable=True)

    # Результат обработки
    success = Column(Boolean, default=True, nullable=False)
    error_message = Column(Text, nullable=True)

    # Временная метка
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    # Связь с пользователем
    user = relationship("User", back_populates="activity_logs")

    def __repr__(self):
        return f"<ActivityLog(telegram_id={self.telegram_id}, action='{self.action}', timestamp={self.timestamp})>"


class BroadcastLog(Base):
    """Лог рассылок"""

    __tablename__ = "broadcast_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Информация о рассылке
    broadcast_id = Column(String(255), nullable=False, index=True)
    message_text = Column(Text, nullable=False)
    sender_id = Column(BigInteger, nullable=False)  # Telegram ID отправителя
    
    # Статистика
    total_users = Column(Integer, default=0)
    sent_successfully = Column(Integer, default=0)
    failed_to_send = Column(Integer, default=0)
    
    # Фильтры
    filters_applied = Column(Text, nullable=True)  # JSON строка
    
    # Статус
    status = Column(String(50), default="pending")  # pending, in_progress, completed, failed
    
    # Временные метки
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<BroadcastLog(id={self.broadcast_id}, status='{self.status}')>"


class SystemStats(Base):
    """Системная статистика для отслеживания производительности"""

    __tablename__ = "system_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Метрики
    metric_name = Column(String(255), nullable=False, index=True)
    metric_value = Column(String(255), nullable=False)
    metric_type = Column(String(50), nullable=False)  # counter, gauge, histogram
    
    # Дополнительные данные
    tags = Column(Text, nullable=True)  # JSON строка с тегами
    
    # Временная метка
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    def __repr__(self):
        return f"<SystemStats(metric_name='{self.metric_name}', value='{self.metric_value}')>"


class WebinarStatus(Base):
    """Статус просмотра вебинара и рассылок ТОЧКА 2"""

    __tablename__ = "webinar_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(
        BigInteger, ForeignKey("users.telegram_id"), nullable=False, index=True
    )

    # Статусы рассылки
    initial_message_sent = Column(Boolean, default=False, nullable=False)  # Отправлено ли первое сообщение
    initial_message_sent_at = Column(DateTime, nullable=True)
    
    # Ответы пользователя на первое сообщение
    watched_webinar = Column(Boolean, nullable=True)  # True - да, False - нет, None - не отвечал
    user_response_at = Column(DateTime, nullable=True)  # Когда пользователь ответил
    
    # Статусы напоминаний
    reminder_scheduled = Column(Boolean, default=False, nullable=False)  # Запланировано ли напоминание
    reminder_sent = Column(Boolean, default=False, nullable=False)  # Отправлено ли напоминание
    reminder_sent_at = Column(DateTime, nullable=True)
    
    # Переходы по ссылке
    link_clicked = Column(Boolean, default=False, nullable=False)  # Переходил ли по ссылке
    link_clicked_at = Column(DateTime, nullable=True)
    
    # Автоматические опросы после 2.5 часов
    auto_survey_scheduled = Column(Boolean, default=False, nullable=False)
    auto_survey_sent = Column(Boolean, default=False, nullable=False)
    auto_survey_sent_at = Column(DateTime, nullable=True)
    
    # Временные метки
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Связь с пользователем
    user = relationship("User", back_populates="webinar_status")

    def __repr__(self):
        return f"<WebinarStatus(telegram_id={self.telegram_id}, watched={self.watched_webinar})>"


class WebinarSurvey(Base):
    """Опрос ТОЧКА 2 - по завершению вебинара (9 вопросов)"""

    __tablename__ = "webinar_surveys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(
        BigInteger, ForeignKey("users.telegram_id"), nullable=False, index=True
    )

    # Вопрос 1: Понятность кардиочекапа
    understanding_cardio_checkup = Column(String(255), nullable=True)
    
    # Вопрос 2: Изменение отношения к профилактике
    attitude_change = Column(String(255), nullable=True)
    
    # Вопрос 3: Проблемы, выявленные при скрининге (множественный выбор)
    screening_problems = Column(Text, nullable=True)  # JSON список
    
    # Вопрос 4: Оценка сердечно-сосудистого риска
    cv_risk_assessment = Column(String(255), nullable=True)
    
    # Вопрос 5: Планируемые действия по модификации образа жизни (множественный выбор)
    lifestyle_actions = Column(Text, nullable=True)  # JSON список
    
    # Вопрос 6: Планы обращения к врачу
    doctor_consultation_plans = Column(String(255), nullable=True)
    
    # Вопрос 7: Мотивация для кардиочекапа (множественный выбор)
    checkup_motivation = Column(Text, nullable=True)  # JSON список
    
    # Вопрос 8: Влияние вебинара на мотивацию
    webinar_influence = Column(String(255), nullable=True)
    
    # Вопрос 9: Самое полезное из вебинара (открытый вопрос)
    most_useful_from_webinar = Column(Text, nullable=True)

    # Временные метки
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Связь с пользователем
    user = relationship("User", back_populates="webinar_surveys")

    def __repr__(self):
        return f"<WebinarSurvey(telegram_id={self.telegram_id}, completed_at={self.completed_at})>"


class FollowUpSurvey(Base):
    """Опрос ТОЧКА 2 и 3 - после вебинара (20 вопросов согласно new_addition.txt)"""

    __tablename__ = "followup_surveys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(
        BigInteger, ForeignKey("users.telegram_id"), nullable=False, index=True
    )

    # ================== ВОПРОСЫ ТОЧКА 2 (сразу после вебинара) ==================

    # Вопрос 1: Понятность кардиочекапа
    understanding_cardio = Column(String(255), nullable=True)

    # Вопрос 2: Изменение отношения к профилактическому обследованию
    attitude_change = Column(String(255), nullable=True)

    # Вопрос 3: Проблемы, выявленные при скрининге (множественный выбор)
    screening_problems = Column(Text, nullable=True)  # JSON список

    # Вопрос 4: Оценка сердечно-сосудистого риска
    cv_risk_assessment = Column(String(255), nullable=True)

    # Вопрос 5: Планируемые действия по модификации образа жизни (множественный выбор)
    lifestyle_plans = Column(Text, nullable=True)  # JSON список

    # Вопрос 6: Планы обращения к врачу
    doctor_plan = Column(String(255), nullable=True)

    # Вопрос 7: Влияние вебинара на мотивацию
    webinar_influence = Column(String(255), nullable=True)

    # ================== ВОПРОСЫ ТОЧКА 3 (через 3+ месяца) ==================

    # Вопрос 8: Обращение к врачу
    doctor_visit = Column(String(255), nullable=True)

    # Вопрос 9: К какому врачу обращались (множественный выбор, до 2)
    doctor_type = Column(Text, nullable=True)  # JSON список

    # Вопрос 10: Отношение врача
    doctor_attitude = Column(String(255), nullable=True)

    # Вопрос 11: Что было сделано на приеме (множественный выбор)
    visit_actions = Column(Text, nullable=True)  # JSON список
    visit_actions_other = Column(Text, nullable=True)  # Поле "Другое"

    # Вопрос 12: Серьезность врача (шкала 1-10)
    doctor_seriousness = Column(Integer, nullable=True)

    # Вопрос 13: Следование рекомендациям
    following_recommendations = Column(String(255), nullable=True)
    following_barriers = Column(Text, nullable=True)  # Свободный ответ

    # Вопрос 14: Шаги по снижению рисков (множественный выбор)
    risk_reduction_steps = Column(Text, nullable=True)  # JSON список
    risk_reduction_other = Column(Text, nullable=True)  # Поле "Другое"

    # Вопрос 15: Стабильность изменений (шкала 0-10)
    changes_stability = Column(Integer, nullable=True)

    # Вопрос 16: Трудности (множественный выбор)
    difficulties = Column(Text, nullable=True)  # JSON список
    difficulties_other = Column(Text, nullable=True)  # Поле "Другое"

    # Вопрос 17: Изменение отношения к профилактике
    prevention_attitude_change = Column(String(255), nullable=True)

    # Вопрос 18: Уверенность в понимании (шкала 0-10)
    understanding_confidence = Column(Integer, nullable=True)

    # Вопрос 19: Потребность в дополнительной информации (множественный выбор)
    additional_info_need = Column(Text, nullable=True)  # JSON список
    additional_info_other = Column(Text, nullable=True)  # Поле "Другое"

    # Вопрос 20: Главное изменение (свободный ответ)
    main_change = Column(Text, nullable=True)

    # Временные метки
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Связь с пользователем
    user = relationship("User", back_populates="followup_surveys")

    def __repr__(self):
        return f"<FollowUpSurvey(telegram_id={self.telegram_id}, completed_at={self.completed_at})>"


class FollowUpStatus(Base):
    """Статус рассылки опроса ТОЧКА 3 (через 3+ месяца)"""

    __tablename__ = "followup_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(
        BigInteger, ForeignKey("users.telegram_id"), nullable=False, index=True
    )

    # Статусы рассылки
    initial_message_sent = Column(Boolean, default=False, nullable=False)  # Отправлено ли начальное сообщение
    initial_message_sent_at = Column(DateTime, nullable=True)

    # Начало опроса
    survey_started = Column(Boolean, default=False, nullable=False)  # Начал ли пользователь опрос
    survey_started_at = Column(DateTime, nullable=True)

    # Завершение опроса
    survey_completed = Column(Boolean, default=False, nullable=False)  # Завершил ли опрос
    survey_completed_at = Column(DateTime, nullable=True)

    # Получение бонуса
    bonus_sent = Column(Boolean, default=False, nullable=False)  # Отправлена ли методичка
    bonus_sent_at = Column(DateTime, nullable=True)

    # Временные метки
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Связь с пользователем
    user = relationship("User", back_populates="followup_status")

    def __repr__(self):
        return f"<FollowUpStatus(telegram_id={self.telegram_id}, survey_completed={self.survey_completed})>"


# Индексы для оптимизации запросов
Index('idx_users_telegram_id', User.telegram_id)
Index('idx_users_last_activity', User.last_activity)
Index('idx_surveys_telegram_id', Survey.telegram_id)
Index('idx_test_results_telegram_id', TestResult.telegram_id)
Index('idx_activity_logs_telegram_id_timestamp', ActivityLog.telegram_id, ActivityLog.timestamp)
Index('idx_broadcast_logs_broadcast_id', BroadcastLog.broadcast_id)
Index('idx_system_stats_metric_timestamp', SystemStats.metric_name, SystemStats.timestamp)
Index('idx_webinar_status_telegram_id', WebinarStatus.telegram_id)
Index('idx_webinar_surveys_telegram_id', WebinarSurvey.telegram_id)
Index('idx_followup_surveys_telegram_id', FollowUpSurvey.telegram_id)
Index('idx_followup_status_telegram_id', FollowUpStatus.telegram_id)