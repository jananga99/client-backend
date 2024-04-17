import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from exceptions.error import Error
from formatters.db import (
    from_db_metadata,
    from_db_node,
    to_db_metadata,
    to_db_node,
)
from schemas.schema import Metadata, SqliteBase, Node
from validators.validators import validate_metadata_id, validate_metadata, validate_node


local_db_url = os.getenv("LOCAL_DB_URL", "sqlite:///local.db")
engine = create_engine(local_db_url)
SqliteBase.metadata.create_all(engine)
Session = sessionmaker(bind=engine)


def insert_metadata(metadata):
    print("Start")
    validate_metadata(metadata, with_id=False)
    session = Session()
    db_metadata = to_db_metadata(metadata)
    session.add(db_metadata)
    session.commit()
    metadata = from_db_metadata(db_metadata)
    session.close()
    return metadata


def get_one_metadata(id):
    validate_metadata_id(id)
    session = Session()
    db_metadata = session.query(Metadata).filter_by(_id=id).first()
    if db_metadata is None:
        raise Error(f"Metadata with id: {id} not found", 404)
    metadata = from_db_metadata(db_metadata)
    session.close()
    return metadata


def get_all_metadata():
    session = Session()
    all_db_metadata = session.query(Metadata).all()
    session.close()
    all_metadata = [from_db_metadata(metadata) for metadata in all_db_metadata]
    for metadata in all_metadata:
        validate_metadata(metadata)
    return all_metadata


def insert_node(node):
    validate_node(node)
    session = Session()
    db_node = to_db_node(node)
    session.add(db_node)
    session.commit()
    node = from_db_node(db_node)
    session.close()
    return node


def get_all_nodes():
    session = Session()
    all_db_nodes = session.query(Node).all()
    session.close()
    all_nodes = [from_db_node(node) for node in all_db_nodes]
    for node in all_nodes:
        validate_node(node)
    return all_nodes
