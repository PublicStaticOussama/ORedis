from core import ORedis

om = ORedis(host='localhost', port=6381, db=0)

from Person import Person


person1 = Person.create({"person_id": 1, "name": 'foo', "email": "foo@bar.com", "age": 10})
person1 = person1.save()

person2 = Person(person_id=2, name="bar", email="bar@baz.com", age=20)
person2 = person2.save()

person3 = Person.create({"person_id": 3, "name": 'baz', "email": "baz@buzz.com", "age": 30})
person3 = person3.save()

people = Person.find({})
print(people)