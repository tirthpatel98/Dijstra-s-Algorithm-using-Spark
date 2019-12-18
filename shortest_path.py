from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def keyValue(arg):
    l=arg.split(":")
    if(len(l)>1):
        return(l)
    else:
        None

def split(string):
    nodes = string[1].split()
    nodes = list(map(int,nodes)) 
    return int(string[0]),(nodes)
        
def newRddFunc(a,b):
    return a,b
     
def returnNeighbours(arg,listOfNode,dist):
    for i in listOfNode:
        if arg[0]==i:
            for j in range(len(arg[1])):
                yield (arg[1][j],(i,dist+1))

def shortestPath(a,b):
    if b[1]<a[1]:
        return b
    else:
        return a
    
def tabSeparated(kv):
    return ("node: "+str(kv[0])+" source "+str(kv[1][0])+","+" distance "+str(kv[1][1]))
    
def main(inputs, output, source, dest):
    if(source!=dest):	#check for different source and dest
    # main logic starts here
        data = sc.textFile(inputs+"/links-simple-sorted.txt")
        rdd = data.map(keyValue)	#generate key value pair
        rdd_path=rdd.map(split).filter(lambda x:x[1] != []).cache()	#splitting nodes on space
        l1=[(source,('-',0))]
        knownPath=sc.parallelize(l1)	#creating rdd for known path

        for i in range(6):
            node = knownPath.filter(lambda x: x[1][1]==i)	#filtering node
            list1=node.map(lambda x:x[0]).collect()		#collecting list of neighbouring 
            newPath = rdd_path.flatMap(lambda x: returnNeighbours(x,list1,i))	#return neighbouring nodes
            knownPath=knownPath.union(newPath)		#joining new path to already known path
            knownPath=knownPath.reduceByKey(shortestPath).cache()	#filtering only the shortest path
            knownPath.map(tabSeparated).coalesce(1).saveAsTextFile(output + '/iter-' + str(i))		#saving intermediate path	
            breakCondition=knownPath.filter(lambda x : x[0]==dest)	
            if len(breakCondition.take(1))!=0:		#checking whether destination is reached
                break

        invalidSource=knownPath.filter(lambda x : x[0]==source)
        invalidDest=knownPath.filter(lambda x : x[0]==dest)
        if len(invalidDest.take(1))!=0 and len(invalidSource.take(1))!=0:	#checking for invalid source or destination
            temp_dest=dest
            path=[dest]

            while(True):		#path tracing
                if temp_dest!=source:
                    temp_path=(knownPath.lookup(temp_dest))
                    temp_dest=temp_path[0][0]
                    path.append(temp_dest)
                else:
                    break
            path.reverse()
            finalPath = sc.parallelize(path)
            finalPath.coalesce(1).saveAsTextFile(output + '/path')
            print(path)
        else:
            print("Source or Destination node not found")
    else:
        print("Source and Destination cannot be same")


if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = int(sys.argv[3])
    dest = int(sys.argv[4])
    #inputs='graph-2'
    #output='output-112'
    #source=12
    #dest=45
    main(inputs, output,source,dest)
