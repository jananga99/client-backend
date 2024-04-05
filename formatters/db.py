def from_db(val):
    val["id"] = str(val["_id"])
    del val["_id"]
    return val

def to_db(val):
    val["_id"] = val["id"]
    del val["id"]
    return val