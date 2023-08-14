from core import ORedisSchema, Schema

@ORedisSchema
class Person(Schema):
    def __init__(self, person_id=0, name="", email="", age=0, employed=False):
        super().__init__()
        self.person_id: int = person_id
        self.name: str = name
        self.email: str = email
        self.age: int = age
        self.employed: bool = employed