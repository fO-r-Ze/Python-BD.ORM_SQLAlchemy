import sqlalchemy
import sqlalchemy as sq
import json
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.sql.expression import text
from sqlalchemy.exc import IntegrityError

# Загружаем пароль от БД из файла
def load_password_from_file(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read()

# Создаем таблицы в БД
def create_tables(engine):
    Base.metadata.create_all(engine)

# Очищаем все таблицы и сбрасываем счётчики последовательностей.
def truncate_all_tables(session, Base):
    metadata = Base.metadata
    tables = reversed(metadata.sorted_tables) # Сортируем таблицы в обратном порядке для соблюдения ограничений FK

    for table in tables:
        # Имя таблицы
        table_name = table.name

        # Удаляем все записи из таблицы
        delete_stmt = table.delete()
        session.execute(delete_stmt)

        # Сбрасываем счётчик последовательности
        seq_name = f"{table_name}_id_seq"
        reset_seq_query = f"ALTER SEQUENCE {seq_name} RESTART WITH 1;"
        session.execute(text(reset_seq_query))

    # Фиксируем изменения
    session.commit()

# Чтение данных из JSON-файла
def load_data_from_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

# Импортируем данные в базу данных
def insert_data(data):
    truncate_all_tables(session, Base)

    with session.no_autoflush:
        try:
            # Определим порядок вставки данных, учитывая зависимости
            models_order = ['publisher', 'shop', 'book', 'stock', 'sale']

            # Проходим по каждому типу модели в нужном порядке
            for model_name in models_order:
                # Получаем соответствующий класс модели по имени
                current_model_class = {
                    'publisher': Publisher,
                    'shop': Shop,
                    'book': Book,
                    'stock': Stock,
                    'sale': Sale,
                }.get(model_name)

                # Фильтруем данные по соответствующему типу модели
                for item in data:
                    if item['model'] == model_name:
                        # Копируем поля (чтобы не изменять оригинал)
                        fields = item['fields'].copy()

                        # Создаем фильтр для проверки существующей записи
                        filter_params = {}
                        for field_name, value in fields.items():
                            filter_params[field_name] = value

                        # Проверяем, существует ли такая запись в базе данных
                        existing_record = session.query(current_model_class).filter_by(**filter_params).first()

                        # Если записи нет, создаём новую
                        if not existing_record:
                            new_data = current_model_class(**fields)
                            session.add(new_data)

            # Завершаем транзакцию и фиксируем изменения
            session.commit()
            print("Данные успешно загружены.")

        except IntegrityError as e:
            # Если возникали ошибки целостности, откатываем транзакцию
            session.rollback()
            print(f'Ошибка вставки данных: {e}')

        finally:
            # Закрываем сессию независимо от результата
            session.close()

# Вывод покупок книг заданного издателя
def find_purchases_by_publisher(publisher_input):
    """
    Выводит покупки книг заданного издателя.
    :param publisher_input: Имя издателя (строка) или его идентификатор (число)
    """

    with Session() as session:
    # Преобразуем ввод в число или строку
        if publisher_input.isdigit():
            publisher_input = int(publisher_input)
            publisher_filter = Publisher.id == publisher_input
        else:
            publisher_filter = Publisher.name == publisher_input

        # Поиск издателя
        publisher = session.query(Publisher).filter(publisher_filter).one_or_none()

        if not publisher:
            print(f"Издатель '{publisher_input}' не найден.")
            return

        # Запрос данных о покупках
        purchases_query = (session.query(
            Book.title, Shop.name, Sale.price, Sale.date_sale)
                           .join(Stock, Stock.id_book == Book.id)
                           .join(Sale, Sale.id_stock == Stock.id)
                           .join(Shop, Stock.id_shop == Shop.id)
                           .filter(Book.id_publisher == publisher.id)
                           .order_by(Sale.date_sale))
        print(f"Покупки книг издателя '{publisher.name}':")

        for title, shop_name, price, date_sale in purchases_query:
            print(f"{title} | {shop_name:15} | {price} | {date_sale.strftime('%d-%m-%Y')}")

password = load_password_from_file('DSN_password.txt')

DSN = "postgresql://postgres:" + password + "@localhost:5432/Python&BD.ORM_SQLAlchemy"
engine = sqlalchemy.create_engine(DSN)

Base = declarative_base()

Session = sessionmaker(bind=engine)
session = Session()

class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=40), unique=True)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher = relationship(Publisher, backref="books")

class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    count = sq.Column(sq.Integer, nullable=False)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)

    book = relationship(Book, backref="stocks")
    shop = relationship(Shop, backref="stocks")

class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.DECIMAL(scale=2), nullable=False)
    date_sale = sq.Column(sq.DateTime(), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)

    stock = relationship(Stock, backref="sales")

if __name__ == "__main__":
    create_tables(engine)

    print("1. Загрузить тестовые данные")
    print("2. Найти покупки по издателю")

    choice = input("Выберите действие (1 или 2): ").strip()
    if choice == "1":
        data = load_data_from_json('tests_data.json')
        insert_data(data)
    elif choice == "2":
        publisher_input = input("Введите имя или идентификатор издателя: ").strip()
        find_purchases_by_publisher(publisher_input)
    else:
        print("Неверный выбор. Завершение программы.")

