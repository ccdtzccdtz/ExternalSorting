
count=0
with open('input//4.csv') as f:
	for line in f:
		count+=1
		try:
			i=int(line.rstrip("\n"))
		except:
			print(count)
			break