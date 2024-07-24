from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

logging.basicConfig(level=logging.DEBUG)

# 数据库连接配置
db_url = "mysql+pymysql://root:root@localhost:3306/simpledb"
engine = create_engine(db_url)
metadata = MetaData()

# 反射数据库表结构
metadata.reflect(engine)

# 使用推荐的方式创建 Base
from sqlalchemy.orm import declarative_base
Base = declarative_base()

def dynamic_table_class(table_name):
    table = metadata.tables.get(table_name)
    if table is None:
        raise ValueError(f"Table '{table_name}' not found in the database.")
    
    # 获取表的主键列
    primary_key_columns = [col.name for col in table.primary_key.columns]

    if not primary_key_columns:
        raise ValueError(f"Table '{table_name}' does not have a primary key.")
    
    logging.debug(f"Primary key columns for table '{table_name}': {primary_key_columns}")

    # 创建一个动态的 ORM 类
    class DynamicRecord(Base):
        __table__ = table
        __mapper_args__ = {
            'primary_key': [table.c[pk] for pk in primary_key_columns]
        }

    return DynamicRecord

# 获取 'record' 表的动态类
Record = dynamic_table_class('record')

# 创建会话
Session = sessionmaker(bind=engine)
session = Session()

# 测试读取数据
try:
    results = session.query(Record).all()
    for row in results:
        logging.debug(f"Record: {row.__dict__}")
except Exception as e:
    logging.error(f"Error querying table 'record': {e}")

# 测试获取列信息
columns = [column.name for column in Record.__table__.columns]
logging.debug(f"Columns in 'record' table: {columns}")
