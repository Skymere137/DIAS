
class Queue:
    def __init__(self, max=5):
        self.values = []
        self.max = max

    def enqueue(self, value):
        if len(self.values) is self.max:
            self.dequeue()
        return self.values.append(value)

    def dequeue(self):
        if self.is_empty():
            return "Nothing in Queue!"
        return self.values.pop(0)
    
    def max_value(self, column, num_of_indices=5):
        if self.is_empty():
            return "Queue is empty!"
        values = []
        for val in self.values:
            values.append(val[column])
        values = values[0:num_of_indices]
        return max(values)
    
    def min_value(self, column, num_of_indices=5):
        if self.is_empty():
            return "Queue is empty!"
        values = []
        for val in self.values:
            values.append(val[column])
        values = values[0:num_of_indices]
        return min(values)
    
    def is_empty(self):
        return len(self.values) == 0

