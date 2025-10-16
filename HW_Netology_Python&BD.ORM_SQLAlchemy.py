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
    with session.no_autoflush:
        try:
            for item in data:
                if item['model'] == 'publisher':
                    existing_publisher = session.query(Publisher).filter_by(name=item['fields']['name']).first()
                    if not existing_publisher:
                        new_data = Publisher(name=item['fields']['name'])
                        session.add(new_data)
            for item in data:
                if item['model'] == 'shop':
                    existing_shop = session.query(Shop).filter_by(name=item['fields']['name']).first()
                    if not existing_shop:
                        new_data = Shop(name=item['fields']['name'])
                        session.add(new_data)
            for item in data:
                if item['model'] == 'book':
                    existing_book = session.query(Book).filter_by(title=item['fields']['title']).first()
                    if not existing_book:
                        new_data = Book(title=item['fields']['title'],
                                        id_publisher=item['fields']['id_publisher'])
                        session.add(new_data)
            for item in data:
                if item['model'] == 'stock':
                    new_data = Stock(count=item['fields']['count'],
                                     id_book=item['fields']['id_book'],
                                     id_shop=item['fields']['id_shop'])
                    session.add(new_data)
            for item in data:
                if item['model'] == 'sale':
                    new_data = Sale(price=item['fields']['price'],
                                    date_sale=item['fields']['date_sale'],
                                    count=item['fields']['count'],
                                    id_stock=item['fields']['id_stock'])
                    session.add(new_data)

        except IntegrityError as e:
            session.rollback()
            print(f'Data insertion failed due to integrity error: {e}')

        session.commit()

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

create_tables(engine)
truncate_all_tables(session, Base)
data = load_data_from_json('tests_data.json')
insert_data(data)


