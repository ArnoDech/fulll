class FizzBuzz:
    def __init__(self):
        self.number = 0

    def get_number(self):
        while True:
            try:
                self.number = int(input("Veuillez insérer un nombre entre 0 et N : "))
                break
            except ValueError:
                print("Error : Please insert valid number")

    def fizz_buzz(self):
        if self.number % 3 == 0 and self.number % 5 == 0:
            return "FizzBuzz"
        elif self.number % 3 == 0:
            return "Fizz"
        elif self.number % 5 == 0:
            return "Buzz"
        else:
            return "no Fizz & no Buzz"

    def run(self):
        while True:
            self.get_number()
            if self.number < 0:
                print("End of the loop")
                break
            result = self.fizz_buzz()
            print(result)
            # Game stopped if number < 0

if __name__ == '__main__':
    test = FizzBuzz()
    test.run()
    print("ok")