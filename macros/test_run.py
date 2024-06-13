class MyPet:
    name: str

    def get_name(self):
        return self.name

    def __call__(self, name):
        self.name = name
