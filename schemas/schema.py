import uuid
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

SqliteBase = declarative_base()


class Metadata(SqliteBase):
    __tablename__ = "metadata"
    _id = Column(String, primary_key=True, default=str(uuid.uuid4().hex))
    name = Column(String)
    size = Column(Integer)
    type = Column(String)
    access_type = Column(String)
    merkel_root = Column(String)
    start_chunk_id = Column(String)
    start_chunk_node_id = Column(Integer)
    created_at = Column(String)
    lastViewed_at = Column(String)


class Node(SqliteBase):
    __tablename__ = "nodes"
    _id = Column(Integer, primary_key=True, autoincrement=True)
    address = Column(String)
    url = Column(String)
