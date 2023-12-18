# when using ORedis it is required to connect to the redis server first using the ORedis class
# otherwise no ORedisSchema would be able to function
# for now ORedis dependens on asyncio coroutines (asyncio plays the same role as Node.js's event loop, coroutines are kind of like Promises in javascript) 
import asyncio
from core import ORedis

om = ORedis(host='localhost', port=6381, db=0, flush=False) # flush=False clears the entire database after connection

# after connecting using the ORedis class it is important to create a schema decorated with the ORedisSchema decorator
# and implementing the Schema interface of ORedis

from core import ORedisSchema, Schema

@ORedisSchema
class Person(Schema):
    # NOTE: it is mandatory to define a default value or type definition like [int(), float(), str(), bool()] in the constructor args in order for ORedis to define the schema correctly on redis-stack
    def __init__(self, person_id=0, name="", email="", age=0, employed=False):
        super().__init__()
        self.person_id: int = person_id
        self.name: str = name
        self.email: str = email
        self.age: int = age
        self.employed: bool = employed

# we define an async main function in order to wrap it with the asyncio event loop later
async def main():
    # there two types of operations possible in ORedis Updates, and Queries

    # Updates:
    # to insert a bulk of instances, you need to define a list of dictionaries, with keys consistent with the fields defined in the schema 
    bulk = [
        {"person_id": 1, "name": 'foo', "email": "foo@bar.com", "age": 10, "employed": True},
        {"person_id": 2, "name": "bar", "email": "bar@baz.com", "age": 20, "employed": False},
        {"person_id": 3, "name": 'baz', "email": "baz@buzz.com", "age": 30, "employed": True},
        {"person_id": 4, "name": 'foo', "email": "foo@bar.com", "age": 10, "employed": False}
    ]

    await Person.insert(bulk)

    # or you can insert one instance in two ways
    # pass a dictionary to the create method
    person1: Person = Person.create({"person_id": 1, "name": 'foo', "email": "foo@bar.com", "age": 10, "employed": True})
    person1 = await person1.save()

    # or create an instance of your schema using its constructor
    person2 = Person(person_id=2, name="bar", email="bar@baz.com", age=20, employed=False)
    person2 = await person2.save()

    # Queries:
    # any query has to be built first by channing either of these functions [one of the 'find' queries, sortBy, limit] using the builder pattern
    # after building a query you need to run .exec() on it in order to get its result
    people: Person = await Person.find({}).exec()
    print("find query:")
    print(people)

# wrapping the asyncio event loop to enable the await keyword
if __name__ == "__main__":
    asyncio.run(main())