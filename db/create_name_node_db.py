
import sys, sqlite3

#taxid : Node
tree = {}

#process the names.dmp
for line in open(sys.argv[1], 'r'):
    fields = line.strip().split("\t")
    notion = fields[6]
    taxid = fields[0]
    name = fields[2]

    if notion== "scientific name":
        if taxid not in tree:
            tree[taxid] = [name,"0",""]
            


#process the nodes.dmp
for line in open(sys.argv[2], 'r'):
    fields = line.strip().split("\t")
    
    taxid = fields[0]
    parent = fields[2]
    rank = fields[4]
    
    if taxid in tree:
        tree.get(taxid)[1] = parent
        tree.get(taxid)[2] = rank
        


conn = sqlite3.connect(sys.argv[3])
cursor = conn.cursor()
cursor.execute("CREATE TABLE tree (taxid integer, name text, parent integer, rank text);")

for taxid in tree.keys():
    command = "INSERT INTO tree VALUES ('" + taxid + "', '" + tree[taxid][0].replace("'","''") + "', '" + tree[taxid][1] + "','" + tree[taxid][2] +"');"

    cursor.execute(command)
    

conn.commit()
