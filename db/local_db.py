import os
import uuid
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from exceptions.error import Error
from formatters.db import from_db_id, from_db_metadata, to_db_id
from validators.validators import validate_metadata_id, validate_metadata, validate_node

Base = declarative_base()


class Metadata(Base):
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


class Node(Base):
    __tablename__ = "nodes"
    _id = Column(Integer, primary_key=True, autoincrement=True)
    address = Column(String)
    url = Column(String)


local_db_url = os.getenv("LOCAL_DB_URL", "sqlite:///local.db")
engine = create_engine(local_db_url)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)


def insert_metadata(metadata):
    validate_metadata(metadata)
    db_metadata = Metadata(**metadata)
    session = Session()
    session.add(db_metadata)
    session.commit()
    metadata["id"] = db_metadata._id
    session.close()


def get_one_metadata(id):
    validate_metadata_id(id)
    session = Session()
    db_metadata = session.query(Metadata).filter_by(_id=id).first()
    session.close()
    if db_metadata is None:
        raise Error(f"Metadata with id: {id} not found", 404)
    metadata = db_metadata.__dict__
    from_db_id(metadata)
    return metadata


def get_all_metadata():
    session = Session()
    all_db_metadata = session.query(Metadata).all()
    session.close()
    all_metadata = [from_db_metadata(metadata) for metadata in all_db_metadata]
    for metadata in all_metadata:
        validate_metadata(metadata, with_id=True)
    return all_metadata


def insert_node(node):
    validate_node(node, with_id=True)
    to_db_id(node)
    session = Session()
    db_node = Node(**node)
    session.add(db_node)
    session.commit()
    session.close()
    validate_node(node, with_db_id=True)
    from_db_id(node)


def get_all_nodes():
    session = Session()
    all_db_nodes = session.query(Node).all()
    session.close()
    all_nodes = [node.__dict__ for node in all_db_nodes]
    for node in all_nodes:
        validate_node(node, with_db_id=True)
        from_db_id(node)
    return all_nodes
