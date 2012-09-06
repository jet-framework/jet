#!/usr/bin/env python
from dateutil.parser import parse
import os

def jobStart(x):
	return "mapred.JobClient: Running job" in x

def jobEnd(x):
	return "mapred.JobClient: Job complete" in x

def format(bytes):
	return "%6.3fGb" % (bytes/1024.0/1024.0/1024.0)
def formatDelta(delta):
	total = delta.total_seconds()
	return "%01d:%02d:%06.03f" % (total/3600, (total % 3600) / 60, total % 60)

def convert(lines):
	out = []
	for line in lines:
		try:
			date = line.split(" ")[1]
			d1 = parse(date)
			out.append( (d1, line))
		except:
			pass
	return out
def allLinesFromFile(filename):
	with open(filename, "r") as f:
		return convert(f)

def totalTime(filename):
	lines = allLinesFromFile(filename)
	dif = lines[-1][0] - lines[0][0]
	return formatDelta(dif)

def jobLengths(filename):
	lengths = []
	for d,line in allLinesFromFile(filename):
		if jobStart(line):
			start = d
		if jobEnd(line):
			lengths.append(formatDelta(d-start))
	return lengths

def jobs(filename):
	count = 0
	for d,line in allLinesFromFile(filename):
		if jobStart(line):
			count += 1
	return count

def sizes(filename):
	written = 0
	read = 0
	for d, line in allLinesFromFile(filename):
		if "FILE_BYTES_WRITTEN" in line:
			written += long(line[line.rindex("=")+1:-1])
		if "FILE_BYTES_READ" in line:
			read += long(line[line.rindex("=")+1:-1])
	return format(read)+"/"+format(written)

def computation(filename):
	start = None
	last = None
	for d, line in allLinesFromFile(filename):
		if "Snappy native library loaded" in line:
			start = d
		if jobEnd(line):
			last = d
	return formatDelta(last-start)

def filterTime(filename):
	laststart = None
	totalTime = None
	for d, line in allLinesFromFile(filename):
		if "Starting filter" in line:
			laststart = d
		if "Ending filter" in line:
			diff = d-laststart
			if totalTime is None:
				totalTime = diff
			else:
				totalTime += diff
	return totalTime
	
def calc( filename):
	d1 = None
	i = 0
	total = None
	out = []
	diffToLast = []
	with open(filename, "r") as f:
		for line in f:

			date = line.split(" ")[1]
			if "Starting" in line:
				if d1 is not None:
					diffToLast.append((parse(date)-d1).total_seconds())
				d1 = parse(date)
			else:
				diff = parse(date) - d1
				if total is None:
					total = diff
				else:
					total += diff
				i+=1
				out.append(diff.total_seconds())
				#print filename, i, diff
			
	#print total
	return out[:-1], diffToLast

def makeFilterPlots():
	mine, mineTotals = calc("mineTimes.txt")
	scoobi, scoobiTotals = calc("scoobiTimes.txt")

	plotdata = "gnuptlotin.txt"
	with open(plotdata, "w") as outFile:
		for x in xrange(0, len(mine)-1):
			outFile.write("%s %s %s %s %s\n" %(x, mine[x], scoobi[x], mineTotals[x], scoobiTotals[x]))
		#x = len(mine)-1
		#outFile.write("%s %s %s\n" %(x, mine[x], scoobi[x]))

def calcSum(data):
	return sum(data)

class Plot():
	def __init__(self, filename=None):
		self.filename = filename
		self.columns = []
		self.title = None
	def addColumn(self, title, i = -1, filename = None):
		self.columns.append((title, i, filename))
	def setTitle(self, title):
		self.title = title
	def preparePlotInput(self, outFile = "plot"):
		out = ""
		out += """set terminal png
set output "%s.png"
set style data linespoints
""" % outFile
		if self.title is not None:
			out += """set title "%s"\n""" % str(self.title)
		plotStr = "plot "
		counter = 2
		first = True
		for title, i, filename in self.columns:
			if (i == -1):
				i = counter
			if filename is None:
				filename = self.filename
			comma = ""
			if not first:
				comma = ", "
			plotStr += """%s "%s" using 1:%s title "%s" """ % (comma, filename, i, title)
			first = False
			counter+=1
		out += plotStr
		return out
	def makePng(self, outFile = "plot"):
		commands = self.preparePlotInput(outFile)
		import subprocess
		plot = subprocess.Popen(['gnuplot'], stdin=subprocess.PIPE)
		plot.communicate(commands)
		plot.wait()
	def writePlotFile(self, outFile = "plot"):
		with open(outFile, "w") as plot:		
			plot.write(self.preparePlotInput(outFile))
		
def makePlots():
	plot = Plot(plotdata)
	plot.addColumn("mine :%s " % calcSum(mine))
	plot.addColumn("scoobi :%s " % calcSum(scoobi))
	plot.addColumn("mine Totals :%s " % calcSum(mineTotals))
	plot.addColumn("scoobi Totals :%s " % calcSum(scoobiTotals))
	plot.setTitle("mine / scoobi = %s" % str(calcSum(mine)/calcSum(scoobi)))
	plot.makePng()
	plot = Plot(plotdata)
	plot.addColumn("mine maps :%s " % calcSum(mine))
	plot.addColumn("scoobi maps :%s " % calcSum(scoobi))
	plot.makePng("plot1")
	plot = Plot(plotdata)
	plot.addColumn("mine Totals :%s " % calcSum(mineTotals), 4)
	plot.addColumn("scoobi Totals :%s " % calcSum(scoobiTotals), 5)
	plot.makePng("plot2")
#for x in listing:
#	print x, fullDiff(path+x), writesize(path+x)
def name(x):
	if "/" in x:
		return x[x.rindex("/")+1:]
	return x
fields = ["   name   ", "jobs", "computation", " totalTime ", " sizes           ", "jobLengths"]
fieldLengths = {"name" : 25}
replacements = {"sizes" : "   read / write  "}
def getLength(i):
	s = fields[i]
	if s.strip() in fieldLengths:
		return fieldLengths[s.strip()]
	return len(s)

def printNice(fields):
	template = ""
	fieldsToPrint = []
	for i, x in enumerate(fields):
		if str(x).strip() in replacements:
			x = replacements[x.strip()]
		template += "{%s:%s} | " % (i, getLength(i))
		fieldsToPrint.append(x)
	template = template[:-3]
	print template.format(*fieldsToPrint)
#, "filterTime"
def printInfos(filename):
	toprint = []
	for field in fields:
		toprint.append(globals()[field.strip()](filename))
	printNice(toprint)
path = "results/"
listing = os.listdir(path)
files = [path+x for x in listing if x.endswith(".txt")]
files = ["manual.txt", "scoobi.txt"]
fieldLengths["name"] = max(4, max([len(name(x)) for x in files]))
files.sort()
printNice(fields)
for x in files:
	printInfos(x)
	#print x, computation(x), fullDiff(x), writesize(x)


