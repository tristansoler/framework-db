import os

for root, dirs, files in os.walk('.\\data_framework\\src\\'):
    if '__init__.py' not in files:
        init_file = os.path.join(root, '__init__.py')
        open(init_file, 'a').close()