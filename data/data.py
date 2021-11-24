import sys

if __name__ == '__main__':
	num_chars = 1024
	for i in range(0, 10):
		with open('file'+str(i), 'w') as f:
			f.write(str(i) * num_chars)
		num_chars += 1024