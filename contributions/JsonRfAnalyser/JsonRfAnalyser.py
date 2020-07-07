#!/usr/bin/python

import json
import sys
import csv
import math

def VarInBranchLimited(tree, uvars, writer, limit):
    """Return all variables in a branch with limit (list)"""
    # Check if the numSplit drop below limit or if it raches a lastSplit node
    if ((len(uvars)+tree['Weigth']) <= limit) or (tree['LastSplit'] == True) :
        vars = uvars[:] + VarInTree(tree)
        vars = list(dict.fromkeys(vars))
        writer.writerow(vars)
        del vars
    else:
        vars = uvars[:] + [tree['splitVar']]
        if 'splitVar' in tree['left']:
            VarInBranchLimited(tree['left'], vars, writer, limit)
        if 'splitVar' in tree['right']:
            VarInBranchLimited(tree['right'], vars, writer, limit)
        del vars
    return

def AddWeigthTree(tree):
    """Add weigth to nodes of a tree (for branch with limit)"""
    if 'splitVar' in tree:
        if 'left' in tree:
            wl = AddWeigthTree(tree['left'])
        if 'right' in tree:
            wr = AddWeigthTree(tree['right'])
        tree['Weigth'] = wl + wr + 1
    else:
        tree['Weigth'] = 0

    if tree['Weigth'] == 1 :
        tree['LastSplit'] = True
    else:
        tree['LastSplit'] = False

    return tree['Weigth']

def VarPerBranchLimited(trees, ofn, limit):
    """Save variables per branch with limit in a line (remove duplicate and comma seprated)"""
    with open(ofn,'w') as csvfile:
        writer = csv.writer(csvfile)
        for i, tree in enumerate(trees):
            if 'splitVar' in tree['rootNode']:
                vars = list()
                vars.append(i)
                tree['Weigth'] = AddWeigthTree(tree['rootNode'])
                VarInBranchLimited(tree['rootNode'], vars, writer, limit)
                  
def VarInBranch(tree, uvars, writer):
    """Return all variables in a branch (list)"""
    vars = uvars[:] + [tree['splitVar']]
    intermediate = False
    if 'splitVar' in tree['left']:
        VarInBranch(tree['left'], vars, writer)
        intermediate = True
    if 'splitVar' in tree['right']:
        VarInBranch(tree['right'], vars, writer)
        intermediate = True
    if intermediate == False:
        #vars = list(dict.fromkeys(vars))
        writer.writerow(vars)
    del vars
    return

def VarPerBranche(trees, ofn):
    """Save variables per branch in a line (remove duplicate and comma seprated)"""
    with open(ofn,'w') as csvfile:
        writer = csv.writer(csvfile)
        for i, tree in enumerate(trees):
            if 'splitVar' in tree['rootNode']:
                vars = list()
                vars.append(i)
                VarInBranch(tree['rootNode'], vars, writer)
                del vars

def VarInTree(tree):
    """Return all variables in a tree (list)"""
    vars = list()
    if 'splitVar' in tree:
        vars += [tree['splitVar']]
        if 'left' in tree:
            vars += VarInTree(tree['left'])
        if 'right' in tree:
            vars += VarInTree(tree['right'])
    return vars

def VarPerTree(trees, ofn):
    """Save variables per tree in a line (remove duplicate and comma seprated)"""
    with open(ofn,'w') as csvfile:
        writer = csv.writer(csvfile)
        for tree in trees:
            treeVars = VarInTree(tree['rootNode'])
            treeVars = list(dict.fromkeys(treeVars))
            writer.writerow(treeVars)

def VarInForest(trees, ofn):
    """Save all variables in the forest (remove duplicate and comma seprated)"""
    forestVars = list()
    for tree in trees:
        treeVars = VarInTree(tree['rootNode'])
        forestVars += treeVars
    forestVars = list(dict.fromkeys(forestVars))
    
    with open(ofn,'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(forestVars)

def ListRoots(trees, ofn):
    """Save roots one per line"""
    with open(ofn,'w') as csvfile:
        writer = csv.writer(csvfile)
        for tree in trees:
            if 'rootNode' in tree and 'splitVar' in tree['rootNode']:
                r = list()
                r.append(tree['rootNode']['splitVar'])
                writer.writerow(r)
 
def Choose(n,r):
  """Computes n! / (r! (n-r)!) exactly. Returns a python long int."""
  assert n >= 0
  assert 0 <= r <= n

  c = 1
  denom = 1
  for (num,denom) in zip(range(n,n-r,-1), range(1,r+1,1)):
    c = (c * num) // denom
  return c

def NumComb(ifn, order, isFirstColIndex=False):
    with open(ifn) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        numComb=0
        for row in readCSV:
            numVar = len(row)
            if(isFirstColIndex):
                numVar -= 1
            if numVar >= order:
                numComb += Choose(numVar,order)
    return numComb

if __name__ == '__main__':
    
    argc = len(sys.argv)
    if(argc < 3):
        print("Enter a command and path to the inptu file")
        exit()

    command = sys.argv[1]
    inputFile = sys.argv[2]
    if(argc > 3):
        outputFile = sys.argv[3]
    thr = -1
    if(argc > 4):
        thr = int(sys.argv[4])

    if(command != "comb" and command != "combx"):
        with open(inputFile, 'r') as json_file:
            full_data = json.load(json_file)

    if(command=="forest"):
        VarInForest(full_data['trees'], outputFile)
    if(command=="tree"):
        VarPerTree(full_data['trees'], outputFile)
    if(command=="branch"):
        VarPerBranche(full_data['trees'], outputFile)
    if(command=="limit"):
        if(thr==-1):
            print("pleas enter limit (4th argument)")
            exit()
        VarPerBranchLimited(full_data['trees'], outputFile, thr)
    if(command=="roots"):
        ListRoots(full_data['trees'], outputFile)
    if(command=="comb" or command=="combx"):
        if(argc < 4):
            print("pleas enter order")
            exit()
        order = int(outputFile) # There is no output file
        excludeFirstCol=False
        if(command=="combx"):
            excludeFirstCol=True
        print(NumComb(inputFile, order, isFirstColIndex=excludeFirstCol))
