from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
from datetime import datetime, time, timedelta
from typing import List, Dict, Any
import traceback
from sqlalchemy import create_engine, text
import uvicorn

app = FastAPI(title="Детали в ОТК")
_db_engines = {}

# Монтируем статические файлы
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def get_db_engine(database="kontakt"):
    """Создает подключение к PostgreSQL через SQLAlchemy"""
    if database == "postgres":
        connection_string = f"postgresql+psycopg2://postgres:sa@192.168.101.12:5432/postgres"
    else:
        connection_string = f"postgresql+psycopg2://postgres:sa@192.168.101.12:5432/kontakt"
    return create_engine(connection_string)

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Главная страница"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/otk-employees")
async def get_otk_employees():
    """Получает список сотрудников ОТК из базы postgres"""
    try:
        engine = get_db_engine("postgres")
        
        # Расширенный запрос с разными вариантами поиска
        query = """
            SELECT 
                u."Description" as employee_name,
                u."idA2" as employee_id,
                a."value1" as department
            FROM "WNUser" u
            INNER JOIN "WNAttributeString" a ON u."idA2" = a."idC2"
            WHERE (
                LOWER(a."value1") LIKE '%отк%' 
                OR LOWER(a."value1") LIKE '%контрол%'
                OR LOWER(a."value1") LIKE '%качеств%'
                OR a."value1" LIKE '%ОТК%'
                OR a."value1" LIKE '%Контрол%'
                OR a."value1" LIKE '%Качеств%'
            )
            AND u."Description" IS NOT NULL
            AND u."Description" != ''
            ORDER BY u."Description"
        """
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
        
        employees = df.to_dict('records')
        print(f"Найдено {len(employees)} сотрудников ОТК")
        
        # Если не нашли, попробуем альтернативный способ
        if len(employees) == 0:
            print("Пробуем альтернативный запрос...")
            return await get_otk_employees_alternative()
        
        return employees
        
    except Exception as e:
        print(f"Ошибка при загрузке сотрудников ОТК: {e}")
        print(traceback.format_exc())
        return await get_otk_employees_alternative()

@app.get("/api/otk-employees-alternative")
async def get_otk_employees_alternative():
    """Альтернативный способ получения сотрудников ОТК"""
    try:
        engine = get_db_engine("postgres")
        
        # Сначала находим ID отделов ОТК
        dept_query = """
            SELECT DISTINCT "idC2" 
            FROM "WNAttributeString" 
            WHERE (
                LOWER("value1") LIKE '%отк%' 
                OR LOWER("value1") LIKE '%контрол%'
                OR LOWER("value1") LIKE '%качеств%'
                OR "value1" LIKE '%ОТК%'
            )
            AND "value1" IS NOT NULL
        """
        
        with engine.connect() as conn:
            dept_df = pd.read_sql_query(text(dept_query), conn)
            dept_ids = dept_df['idC2'].tolist()
        
        print(f"Найдено ID отделов: {dept_ids}")
        
        if not dept_ids:
            # Если отделы не найдены, возвращаем всех пользователей с Description
            all_users_query = """
                SELECT "Description" as employee_name
                FROM "WNUser" 
                WHERE "Description" IS NOT NULL
                AND "Description" != ''
                ORDER BY "Description"
                LIMIT 50
            """
            with engine.connect() as conn:
                users_df = pd.read_sql_query(text(all_users_query), conn)
            
            employees = users_df.to_dict('records')
            print(f"Возвращаем всех пользователей ({len(employees)} записей)")
            return employees
        
        # Ищем сотрудников по найденным ID отделов
        employees_query = """
            SELECT "Description" as employee_name
            FROM "WNUser" 
            WHERE "idA2" IN ({})
            AND "Description" IS NOT NULL
            AND "Description" != ''
            ORDER BY "Description"
        """.format(','.join(['%s'] * len(dept_ids)))
        
        with engine.connect() as conn:
            employees_df = pd.read_sql_query(text(employees_query), conn, params=dept_ids)
        
        employees = employees_df.to_dict('records')
        print(f"Альтернативный метод: найдено {len(employees)} сотрудников")
        
        return employees
        
    except Exception as e:
        print(f"Ошибка альтернативного метода: {e}")
        return []

@app.get("/api/debug-otk-employees")
async def debug_otk_employees():
    """Отладочный endpoint для проверки данных сотрудников ОТК"""
    try:
        engine = get_db_engine("postgres")
        
        # Сначала проверяем что находим в WNAttributeString
        attribute_query = """
            SELECT "idC2", "value1", "value2"
            FROM "WNAttributeString" 
            WHERE "value1" LIKE '%ОТК%'
            LIMIT 10
        """
        
        # Затем полный запрос
        full_query = """
            SELECT 
                u."Description" as employee_name,
                u."idA2" as employee_id,
                a."value1" as attribute_value
            FROM "WNUser" u
            INNER JOIN "WNAttributeString" a ON u."idA2" = a."idC2"
            WHERE a."value1" LIKE '%ОТК%'
            AND u."Description" IS NOT NULL
            ORDER BY u."Description"
        """
        
        with engine.connect() as conn:
            attribute_df = pd.read_sql_query(text(attribute_query), conn)
            full_df = pd.read_sql_query(text(full_query), conn)
        
        return {
            "attributes_found": attribute_df.to_dict('records'),
            "employees_found": full_df.to_dict('records'),
            "count": len(full_df)
        }
        
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/api/check-tables")
async def check_tables():
    """Проверяем структуру таблиц в базе postgres"""
    try:
        engine = get_db_engine("postgres")
        
        # Проверяем существование таблиц и столбцов
        check_query = """
            SELECT 
                table_name,
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns 
            WHERE table_name IN ('WNAttributeString', 'WNUser')
            ORDER BY table_name, ordinal_position
        """
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(check_query), conn)
        
        return {
            "table_structure": df.to_dict('records'),
            "message": f"Найдено {len(df)} столбцов в таблицах"
        }
        
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/api/debug-tables-content")
async def debug_tables_content():
    """Проверяем содержимое таблиц"""
    try:
        engine = get_db_engine("postgres")
        
        # Проверяем WNAttributeString
        attribute_query = """
            SELECT "idC2", "value1", "value2" 
            FROM "WNAttributeString" 
            WHERE "value1" IS NOT NULL
            LIMIT 20
        """
        
        # Проверяем WNUser
        user_query = """
            SELECT "idA2", "Description" 
            FROM "WNUser" 
            WHERE "Description" IS NOT NULL
            LIMIT 20
        """
        
        with engine.connect() as conn:
            attribute_df = pd.read_sql_query(text(attribute_query), conn)
            user_df = pd.read_sql_query(text(user_query), conn)
        
        return {
            "WNAttributeString_samples": attribute_df.to_dict('records'),
            "WNUser_samples": user_df.to_dict('records'),
            "attribute_count": len(attribute_df),
            "user_count": len(user_df)
        }
        
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/api/data")
async def get_otk_queue() -> List[Dict[str, Any]]:
    """
    Объединяем данные из KQCDTasks и KOperations по barcode для определения приоритетов
    """
    try:
        engine = get_db_engine()
        
        # ОСНОВНОЙ ЗАПРОС - правильное соединение по barcode
        query = """
            SELECT 
                kt.id,
                kt."orderNumber" as order_number,
                kt."partName" as part_name,
                kt."machineName" as machine_name,
                kt.operator,
                kt."dateFinish" as date_finish,
                kt."operatorAmount" as quantity,
                COALESCE(ko."isPriority", FALSE) as is_critical_priority,
                kt.barcode,
                kt."qcdUser"  -- Добавляем для отладки
            FROM "KQCDTasks" kt
            LEFT JOIN "KOperations" ko 
                ON kt.barcode = ko.barcode
            WHERE (kt."qcdUser" IS NULL OR kt."qcdUser" = '')
            AND kt."dateFinish" IS NOT NULL
            AND kt."dateFinish" >= '2025-09-15'
            AND kt."operatorAmount" > 0
            ORDER BY 
                CASE WHEN ko."isPriority" = TRUE THEN 0 ELSE 1 END,
                kt."dateFinish" ASC
            LIMIT 200
        """
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
        
        # Преобразуем NULL в False для приоритета
        df['is_critical_priority'] = df['is_critical_priority'].fillna(False)
        
        data = df.to_dict('records')
        critical_count = df['is_critical_priority'].sum()
        print(f"Найдено {len(data)} деталей в ОТК, из них критических: {critical_count}")
        
        # Отладочная информация
        if critical_count > 0:
            critical_items = df[df['is_critical_priority'] == True][['barcode', 'order_number', 'part_name', 'qcdUser']].to_dict('records')
            print("Критические позиции в результате:", critical_items)
        else:
            print("Критических позиций не найдено в результате запроса")
            # Проверим почему
            debug_query = """
                SELECT kt.barcode, kt."qcdUser", ko."isPriority"
                FROM "KQCDTasks" kt
                INNER JOIN "KOperations" ko ON kt.barcode = ko.barcode
                WHERE ko."isPriority" = TRUE
                LIMIT 5
            """
            with engine.connect() as conn:
                debug_df = pd.read_sql_query(text(debug_query), conn)
                print("Приоритетные позиции в БД (все):", debug_df.to_dict('records'))
        
        return data
        
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        print(traceback.format_exc())
        return []

@app.get("/api/check-specific")
async def check_specific():
    """Проверяем конкретную приоритетную позицию"""
    try:
        engine = get_db_engine()
        
        query = """
            SELECT 
                kt.id,
                kt.barcode,
                kt."orderNumber",
                kt."partName", 
                kt."qcdUser",
                kt."dateFinish",
                kt."operatorAmount",
                ko."isPriority"
            FROM "KQCDTasks" kt
            LEFT JOIN "KOperations" ko ON kt.barcode = ko.barcode
            WHERE kt.barcode = '20250820-3952-1-5'
        """
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
        
        return {
            "specific_item": df.to_dict('records'),
            "explanation": "Если qcdUser не пустой - позиция уже проверена и не показывается"
        }
        
    except Exception as e:
        return {"error": str(e)}


    
@app.get("/api/all-priority-items")
async def get_all_priority_items():
    """Получить все приоритетные позиции без фильтров"""
    try:
        engine = get_db_engine()
        
        query = """
            SELECT 
                kt.id,
                kt.barcode,
                kt."orderNumber",
                kt."partName", 
                kt."qcdUser",
                kt."dateFinish",
                kt."operatorAmount",
                ko."isPriority"
            FROM "KQCDTasks" kt
            INNER JOIN "KOperations" ko ON kt.barcode = ko.barcode
            WHERE ko."isPriority" = TRUE
            ORDER BY kt."dateFinish" DESC
        """
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
        
        return {
            "all_priority_items": df.to_dict('records'),
            "count": len(df),
            "with_qcdUser_null": len(df[df['qcdUser'].isna() | (df['qcdUser'] == '')])
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/stats")
async def get_stats():
    """Возвращает статистику по деталям в ОТК"""
    try:
        engine = get_db_engine()
        
        # Получаем текущую дату (без времени)
        today = datetime.now().date()
        
        # Запрос для непроверенных деталей (ожидающих ОТК)
        query_unchecked = """
            SELECT COUNT(*) as total
            FROM "KQCDTasks" 
            WHERE ("qcdUser" IS NULL OR "qcdUser" = '')
            AND "dateFinish" IS NOT NULL
            AND "dateFinish" >= '2025-09-15'
            AND "operatorAmount" > 0
        """
        
        # Запрос для проверенных СЕГОДНЯ (по новой логике)
        query_checked_today = """
            SELECT COUNT(*) as checked_today
            FROM "KQCDTasks" 
            WHERE "qcdUser" IS NOT NULL 
            AND "qcdUser" != ''
            AND "operatorAmount" > 0
            AND "qcdDateFinish" IS NOT NULL
            AND DATE("qcdDateFinish") = :today
        """
        
        with engine.connect() as conn:
            # Получаем количество непроверенных позиций
            result_unchecked = conn.execute(text(query_unchecked))
            total = result_unchecked.scalar()
            
            # Получаем количество проверенных СЕГОДНЯ позиций
            result_checked = conn.execute(text(query_checked_today), {'today': today})
            checked_today = result_checked.scalar()
        
        print(f"Статистика: {total} ожидают, {checked_today} проверено сегодня")
        
        return {
            "total": total,
            "checked_today": checked_today,
            "updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"Ошибка статистики: {e}")
        print(traceback.format_exc())
        return {"total": 0, "checked_today": 0, "updated": datetime.now().isoformat()}

@app.get("/api/debug-priority")
async def debug_priority():
    """Отладочный endpoint для проверки приоритетных позиций"""
    try:
        engine = get_db_engine()
        
        # Проверяем есть ли приоритетные позиции в KOperations
        priority_query = """
            SELECT barcode, "isPriority", "orderNumber", "partName"
            FROM "KOperations" 
            WHERE "isPriority" = TRUE
            LIMIT 10
        """
        
        # Проверяем соединение с KQCDTasks
        join_query = """
            SELECT 
                kt.barcode as kt_barcode,
                ko.barcode as ko_barcode,
                kt."orderNumber" as kt_order,
                ko."orderNumber" as ko_order,
                ko."isPriority"
            FROM "KQCDTasks" kt
            INNER JOIN "KOperations" ko ON kt.barcode = ko.barcode
            WHERE ko."isPriority" = TRUE
            LIMIT 10
        """
        
        with engine.connect() as conn:
            priority_df = pd.read_sql_query(text(priority_query), conn)
            join_df = pd.read_sql_query(text(join_query), conn)
        
        return {
            "priority_in_koperations": priority_df.to_dict('records'),
            "joined_data": join_df.to_dict('records'),
            "message": f"Найдено {len(priority_df)} приоритетных в KOperations, {len(join_df)} совпадений с KQCDTasks"
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/today-stats")
async def get_today_stats():
    """Возвращает детальную статистику по проверенным сегодня счетам"""
    try:
        engine = get_db_engine()
        
        # Получаем текущую дату (без времени)
        today = datetime.now().date()
        
        # ОБНОВЛЕННЫЙ ЗАПРОС - добавляем станок и оператора
        query = """
            SELECT 
                "orderNumber" as order_number,
                "partName" as part_name,
                "machineName" as machine_name,
                operator,
                "operatorAmount" as quantity,
                "qcdUser" as qcd_user,
                "qcdDateFinish" as qcd_date_finish
            FROM "KQCDTasks" 
            WHERE "qcdUser" IS NOT NULL 
            AND "qcdUser" != ''
            AND "operatorAmount" > 0
            AND "qcdDateFinish" IS NOT NULL
            AND DATE("qcdDateFinish") = :today
            ORDER BY "qcdDateFinish" DESC
        """
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn, params={'today': today})
        
        # Подсчет общей статистики
        total_positions = len(df)  # Количество проверенных позиций
        total_parts = df['quantity'].sum()  # Общее количество деталей
        
        # Статистика по сотрудникам
        users_stats = df.groupby('qcd_user').agg({
            'order_number': 'count',  # Количество позиций у каждого сотрудника
            'quantity': 'sum'
        }).reset_index()
        users_stats = users_stats.rename(columns={
            'order_number': 'position_count',
            'quantity': 'part_count'
        })
        
        return {
            "total_positions": int(total_positions),
            "total_parts": int(total_parts),
            "users": users_stats.to_dict('records'),
            "orders": df.to_dict('records')
        }
        
    except Exception as e:
        print(f"Ошибка при загрузке сегодняшней статистики: {e}")
        print(traceback.format_exc())
        return {"total_positions": 0, "total_parts": 0, "users": [], "orders": []}
    
@app.get("/api/employee-stats")
async def get_employee_stats(days: int = 7):
    """Возвращает статистику по сотрудникам за указанный период"""
    try:
        engine = get_db_engine()
        start_date = (datetime.now() - timedelta(days=days)).date()
        
        # ИСПРАВЛЕННЫЙ ЗАПРОС
        query = """
            SELECT 
                "qcdUser" as qcd_user,
                DATE("qcdDateFinish") as check_date,
                COUNT(DISTINCT id) as position_count,
                COALESCE(SUM("qcdAmount"), 0) as part_count
            FROM "KQCDTasks" 
            WHERE "qcdUser" IS NOT NULL 
            AND "qcdUser" != ''
            AND "qcdDateFinish" IS NOT NULL
            AND DATE("qcdDateFinish") >= :start_date
            GROUP BY "qcdUser", DATE("qcdDateFinish")
            ORDER BY check_date DESC, qcd_user
        """
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn, params={'start_date': start_date})
        
        # Общая статистика по сотрудникам (за весь период)
        total_stats_query = """
            SELECT 
                "qcdUser" as qcd_user,
                COUNT(DISTINCT id) as position_count,
                COALESCE(SUM("qcdAmount"), 0) as part_count
            FROM "KQCDTasks" 
            WHERE "qcdUser" IS NOT NULL 
            AND "qcdUser" != ''
            AND "qcdDateFinish" IS NOT NULL
            AND DATE("qcdDateFinish") >= :start_date
            GROUP BY "qcdUser"
            ORDER BY position_count DESC
        """
        
        with engine.connect() as conn:
            total_stats_df = pd.read_sql_query(text(total_stats_query), conn, params={'start_date': start_date})
        
        print(f"Загружено {len(df)} записей статистики за {days} дней")
        print(f"Уникальных сотрудников: {total_stats_df['qcd_user'].nunique()}")
        
        return {
            "daily_stats": df.to_dict('records'),
            "total_stats": total_stats_df.to_dict('records'),
            "period_days": days
        }
        
    except Exception as e:
        print(f"Ошибка при загрузке статистики сотрудников: {e}")
        print(traceback.format_exc())
        return {"daily_stats": [], "total_stats": [], "period_days": days}

@app.get("/api/employee-data/{employee_name}")
async def get_employee_data(employee_name: str, days: int = 1):
    """Возвращает данные для конкретного сотрудника ОТК"""
    try:
        engine = get_db_engine()
        
        # Декодируем имя сотрудника
        employee_name_decoded = employee_name.replace('_', ' ')
        
        # Ожидающие проверки детали (все)
        waiting_query = """
            SELECT 
                "orderNumber" as order_number,
                "partName" as part_name,
                "machineName" as machine_name,
                operator,
                "dateFinish" as date_finish,
                "operatorAmount" as quantity,
                FALSE as is_critical_priority  -- Заглушка, нужно доработать
            FROM "KQCDTasks" 
            WHERE ("qcdUser" IS NULL OR "qcdUser" = '')
            AND "dateFinish" IS NOT NULL
            AND "dateFinish" >= '2025-09-15'
            AND "operatorAmount" > 0
            ORDER BY "dateFinish" ASC
            LIMIT 50
        """
        
        # Проверенные детали сотрудником
        checked_query = """
            SELECT 
                "orderNumber" as order_number,
                "partName" as part_name,
                "machineName" as machine_name,
                "operatorAmount" as quantity,
                "qcdDateFinish" as qcd_date_finish
            FROM "KQCDTasks" 
            WHERE "qcdUser" = :employee_name
            AND "operatorAmount" > 0
            AND "qcdDateFinish" IS NOT NULL
            AND DATE("qcdDateFinish") >= CURRENT_DATE - INTERVAL ':days days'
            ORDER BY "qcdDateFinish" DESC
            LIMIT 50
        """
        
        with engine.connect() as conn:
            waiting_df = pd.read_sql_query(text(waiting_query), conn)
            checked_df = pd.read_sql_query(text(checked_query), conn, 
                                         params={'employee_name': employee_name_decoded, 'days': days})
        
        return {
            "waiting_parts": waiting_df.to_dict('records'),
            "checked_parts": checked_df.to_dict('records'),
            "employee_name": employee_name_decoded
        }
        
    except Exception as e:
        print(f"Ошибка при загрузке данных сотрудника {employee_name}: {e}")
        return {"waiting_parts": [], "checked_parts": [], "employee_name": employee_name_decoded}


@app.get("/api/employee-checked-parts/{employee_name}")
async def get_employee_checked_parts(employee_name: str):
    """Возвращает все проверенные детали сотрудника (поиск по фамилии)"""
    try:
        engine = get_db_engine()
        import urllib.parse
        employee_name_decoded = urllib.parse.unquote(employee_name)
        
        # Извлекаем только фамилию (первое слово)
        surname = employee_name_decoded.split()[0] if employee_name_decoded.split() else employee_name_decoded
        
        print(f"🔍 Поиск проверенных деталей для фамилии: '{surname}'")
        print(f"📝 Полное имя из запроса: '{employee_name_decoded}'")
        
        # Ищем по фамилии (начало строки)
        query = """
            SELECT 
                "orderNumber" as order_number,
                "partName" as part_name,
                "machineName" as machine_name,
                operator,
                "dateStart" as date_start,
                "dateFinish" as date_finish,
                "operatorAmount" as operator_amount,  -- Количество ПЕРЕД qcdAmount
                "qcdAmount" as qcd_amount,
                "qcdDefect" as qcd_defect,
                "qcdComment" as qcd_comment,
                "qcdDateFinish" as qcd_date_finish,
                "qcdUser"
            FROM "KQCDTasks" 
            WHERE "qcdUser" LIKE :surname_pattern
            AND "qcdDateFinish" IS NOT NULL
            ORDER BY "qcdDateFinish" DESC
            LIMIT 200
        """
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn, 
                                 params={'surname_pattern': f'{surname}%'})
        
        print(f"✅ Найдено {len(df)} записей для фамилии '{surname}'")
        
        # Выведем примеры найденных qcdUser для отладки
        if len(df) > 0:
            unique_users = df['qcdUser'].unique() if 'qcdUser' in df.columns else []
            print(f"👥 Найденные сотрудники в БД: {list(unique_users)}")
        
        return {
            "checked_parts": df.to_dict('records'),
            "employee_name": employee_name_decoded,
            "surname_used": surname,
            "total_count": len(df)
        }
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке проверенных деталей: {e}")
        print(traceback.format_exc())
        return {"checked_parts": [], "employee_name": employee_name, "total_count": 0}

@app.get("/api/debug-employee-search/{employee_name}")
async def debug_employee_search(employee_name: str):
    """Отладочный endpoint для поиска сотрудников"""
    try:
        engine = get_db_engine()
        import urllib.parse
        employee_name_decoded = urllib.parse.unquote(employee_name)
        surname = employee_name_decoded.split()[0] if employee_name_decoded.split() else employee_name_decoded
        
        print(f"🔍 Отладочный поиск: '{employee_name_decoded}' -> фамилия: '{surname}'")
        
        # Ищем всех сотрудников с этой фамилией
        query = """
            SELECT DISTINCT 
                "qcdUser",
                COUNT(*) as total_records
            FROM "KQCDTasks" 
            WHERE "qcdUser" LIKE :surname_pattern
            AND "qcdDateFinish" IS NOT NULL
            GROUP BY "qcdUser"
            ORDER BY total_records DESC
        """
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn, 
                                 params={'surname_pattern': f'{surname}%'})
        
        return {
            "original_name": employee_name_decoded,
            "surname_used": surname,
            "found_employees": df.to_dict('records'),
            "search_pattern": f"{surname}%"
        }
        
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/debug-qcd-users")
async def debug_qcd_users():
    """Отладочный endpoint для проверки сотрудников ОТК в базе"""
    try:
        engine = get_db_engine()
        
        query = """
            SELECT DISTINCT 
                "qcdUser",
                COUNT(*) as total_checks,
                MIN("qcdDateFinish") as first_check,
                MAX("qcdDateFinish") as last_check
            FROM "KQCDTasks" 
            WHERE "qcdUser" IS NOT NULL 
            AND "qcdUser" != ''
            AND "qcdDateFinish" IS NOT NULL
            GROUP BY "qcdUser"
            ORDER BY total_checks DESC
            LIMIT 50
        """
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
        
        return {
            "qcd_users": df.to_dict('records'),
            "total_unique_users": len(df)
        }
        
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

    
@app.get("/api/operators-stats")
async def get_operators_stats(days: int = 30, machine: str = "all"):
    """Возвращает статистику по операторам за указанный период"""
    try:
        engine = get_db_engine()
        start_date = (datetime.now() - timedelta(days=days)).date()
        
        # Улучшенный запрос с проверкой дат
        base_query = """
            SELECT 
                operator,
                "machineName" as machine_name,
                MIN("dateStart") as date_start,
                MAX("dateFinish") as date_finish,
                SUM("operatorAmount") as produced,
                COALESCE(SUM("qcdAmount"), 0) as accepted,
                COALESCE(SUM("qcdDefect"), 0) as defects
            FROM "KQCDTasks" 
            WHERE operator IS NOT NULL 
            AND operator != ''
            AND "operatorAmount" > 0
            AND "dateFinish" IS NOT NULL
            AND DATE("dateFinish") >= :start_date
        """
        
        params = {'start_date': start_date}
        if machine != "all":
            base_query += " AND \"machineName\" = :machine_name"
            params['machine_name'] = machine
        
        base_query += " GROUP BY operator, \"machineName\""
        
        with engine.connect() as conn:
            df = pd.read_sql_query(text(base_query), conn, params=params)
        
        if df.empty:
            return {
                "operators_stats": [],
                "summary": {
                    "total_operators": 0,
                    "total_produced": 0,
                    "total_accepted": 0,
                    "total_defects": 0,
                    "avg_quality": 0
                },
                "analysis": {},
                "machines": [],
                "period_days": days
            }
        
        # Рассчитываем качество (избегаем деление на ноль)
        df['quality_rate'] = df.apply(
            lambda x: round((x['accepted'] / x['produced'] * 100), 2) if x['produced'] > 0 else 0, 
            axis=1
        )
        df['defect_rate'] = df.apply(
            lambda x: round((x['defects'] / x['produced'] * 100), 2) if x['produced'] > 0 else 0, 
            axis=1
        )
        
        # Сортируем по качеству
        df = df.sort_values('quality_rate', ascending=False)
        
        # Группируем по оператору и станку
        grouped_stats = df.groupby(['operator', 'machine_name']).agg({
            'produced': 'sum',
            'accepted': 'sum',
            'defects': 'sum',
            'date_start': 'min',
            'date_finish': 'max'
        }).reset_index()
        
        # Рассчитываем дополнительные метрики
        grouped_stats['quality_rate'] = (grouped_stats['accepted'] / grouped_stats['produced'] * 100).round(2)
        grouped_stats['defect_rate'] = (grouped_stats['defects'] / grouped_stats['produced'] * 100).round(2)
        grouped_stats['efficiency'] = (grouped_stats['accepted'] / grouped_stats['produced'] * 100).round(2)
        
        # Сортируем по эффективности
        grouped_stats = grouped_stats.sort_values('efficiency', ascending=False)
        
        # Общая статистика
        total_produced = grouped_stats['produced'].sum()
        total_accepted = grouped_stats['accepted'].sum()
        total_defects = grouped_stats['defects'].sum()
        avg_quality = (total_accepted / total_produced * 100) if total_produced > 0 else 0
        
        # Анализ эффективности
        analysis = {}
        if not grouped_stats.empty:
            best_operator = grouped_stats.iloc[0]
            worst_operator = grouped_stats.iloc[-1]
            
            analysis = {
                "best_operator": {
                    "name": best_operator['operator'],
                    "machine": best_operator['machine_name'],
                    "quality": best_operator['quality_rate'],
                    "produced": int(best_operator['produced'])
                },
                "worst_operator": {
                    "name": worst_operator['operator'],
                    "machine": worst_operator['machine_name'],
                    "quality": worst_operator['quality_rate'],
                    "produced": int(worst_operator['produced'])
                },
                "avg_quality": round(avg_quality, 2)
            }
        
        # Получаем список станков для фильтра
        machines_query = "SELECT DISTINCT \"machineName\" FROM \"KQCDTasks\" WHERE \"machineName\" IS NOT NULL"
        with engine.connect() as conn:
            machines_df = pd.read_sql_query(text(machines_query), conn)
        machines_list = machines_df['machineName'].tolist()
        
        print(f"Статистика операторов: {len(grouped_stats)} записей за {days} дней")
        
        return {
            "operators_stats": grouped_stats.to_dict('records'),
            "summary": {
                "total_operators": len(grouped_stats),
                "total_produced": int(total_produced),
                "total_accepted": int(total_accepted),
                "total_defects": int(total_defects),
                "avg_quality": round(avg_quality, 2)
            },
            "analysis": analysis,
            "machines": machines_list,
            "period_days": days
        }
        
    except Exception as e:
        print(f"Ошибка при загрузке статистики операторов: {e}")
        print(traceback.format_exc())
        return {
            "operators_stats": [],
            "summary": {
                "total_operators": 0,
                "total_produced": 0,
                "total_accepted": 0,
                "total_defects": 0,
                "avg_quality": 0
            },
            "analysis": {},
            "machines": [],
            "period_days": days
        }
@app.get("/api/test-relation")
async def test_relation():
    """Тестируем связь между таблицами"""
    try:
        engine = get_db_engine("postgres")
        
        # Проверяем есть ли общие ID между таблицами
        relation_query = """
            SELECT 
                COUNT(*) as total_users,
                COUNT(DISTINCT u."idA2") as unique_user_ids,
                COUNT(DISTINCT a."idC2") as unique_attribute_ids,
                COUNT(CASE WHEN u."idA2" = a."idC2" THEN 1 END) as matching_ids
            FROM "WNUser" u
            CROSS JOIN "WNAttributeString" a
            WHERE u."Description" IS NOT NULL
            AND a."value1" IS NOT NULL
            LIMIT 1000
        """
        
        # Более простой тест
        simple_test_query = """
            SELECT 
                u."idA2" as user_id,
                u."Description" as user_name,
                a."idC2" as attr_id, 
                a."value1" as department
            FROM "WNUser" u
            INNER JOIN "WNAttributeString" a ON u."idA2" = a."idC2"
            WHERE u."Description" IS NOT NULL
            AND a."value1" IS NOT NULL
            LIMIT 10
        """
        
        with engine.connect() as conn:
            relation_df = pd.read_sql_query(text(relation_query), conn)
            simple_df = pd.read_sql_query(text(simple_test_query), conn)
        
        return {
            "relation_stats": relation_df.to_dict('records'),
            "sample_relations": simple_df.to_dict('records'),
            "message": "Тест связи таблиц выполнен"
        }
        
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

# Endpoint для сохранения настроек на сервере
@app.post("/api/save-employee-mappings")
async def save_employee_mappings(request: Request):
    """Сохраняет настройки сотрудников на сервере"""
    try:
        data = await request.json()
        employee_mappings = data.get("employee_mappings", {})
        hidden_employees = data.get("hidden_employees", [])
        
        # Здесь можно сохранить в базу данных, файл или просто в память
        # Для простоты сохраним в глобальные переменные (в продакшене используйте БД)
        global SERVER_EMPLOYEE_MAPPINGS, SERVER_HIDDEN_EMPLOYEES
        SERVER_EMPLOYEE_MAPPINGS = employee_mappings
        SERVER_HIDDEN_EMPLOYEES = hidden_employees
        
        print("✅ Настройки сохранены на сервере")
        print(f"   Mappings: {len(employee_mappings)} записей")
        print(f"   Hidden: {len(hidden_employees)} сотрудников")
        
        return {"status": "success", "message": "Настройки сохранены"}
        
    except Exception as e:
        print(f"❌ Ошибка сохранения настроек: {e}")
        return {"status": "error", "message": str(e)}

# Endpoint для загрузки настроек с сервера
@app.get("/api/load-employee-mappings")
async def load_employee_mappings():
    """Загружает настройки сотрудников с сервера"""
    try:
        # Используем глобальные переменные (в продакшене - БД)
        global SERVER_EMPLOYEE_MAPPINGS, SERVER_HIDDEN_EMPLOYEES
        
        return {
            "status": "success",
            "employee_mappings": SERVER_EMPLOYEE_MAPPINGS or {},
            "hidden_employees": SERVER_HIDDEN_EMPLOYEES or []
        }
        
    except Exception as e:
        print(f"❌ Ошибка загрузки настроек: {e}")
        return {"status": "error", "employee_mappings": {}, "hidden_employees": []}

# Инициализация глобальных переменных (добавьте в начало файла)
SERVER_EMPLOYEE_MAPPINGS = {}
SERVER_HIDDEN_EMPLOYEES = []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="192.168.101.143", port=8503)
