import os
import sys
from operator import itemgetter
import heapq
import threading
import time

start_time = time.time()

MEMORY_LIMIT = 0                            #initialise memory limit from user custom preference
THREAD_COUNT = 0                            #number of threads to be used : will be provided by user
INPUT_FILE = 'input.txt'
OUTPUT_FILE = 'output.txt'
TUPLE_SIZE = 0
TOTAL_TUPLES = 0                            #num of records in the input file
TOTAL_FILESIZE = 0                          #input file size = TOTAL_TUPLES * TUPLE_SIZE
TOTAL_SUBFILES = 0     #num of sub-files required to store input data as memory size is less : ceil(TOTAL_FILESIZE / MEMORY_LIMIT)
TUPLES_PER_SUBFILE = 0      #num of tuples that can fit in each sub-file : floor(MEMORY_LIMIT / TUPLE_SIZE)
ROW_LEN = 0
TUPLES_PER_THREAD = 0                       #

cols_to_sort = []                           #a list of col_names provided by user to be sorted
col_index_to_sort = []                      #actual col_indices in input file that need to be sorted
thread_reqd = False                         #based  on user input if threads are req. or not
asc = False                                 #sort based on asc or desc
col_details = dict()                        #store <col_name, [col_index, col_width]> in dict. e.g : <c1, [0,10]>
file_counter = 0


#this will represent records that can be compared and sorted based on req cols : just like custom operator
class heap_object(object):
    def __init__(self, val, filename):
        self.val = val
        self.filename = filename
    def __lt__(self, other):
        for i in col_index_to_sort:
            if(asc == True):
                if(self.val[i] < other.val[i]):
                    return True
                elif(self.val[i] > other.val[i]):
                    return False
            else:
                if(self.val[i] > other.val[i]):
                    return True
                elif(self.val[i] < other.val[i]):
                    return False
        return False
    



#read the args provided by user and initialise global variables
def check_args(args):
    print('[CHECKING] Arguments')
    if(len(args) < 6):                                      #atleast 6 args required
        print('[ERROR] Invalid Argument Count')
        sys.exit(1)

    #if you want the variables declared inside a function(by default local) to be golbal => use global keyword
    global MEMORY_LIMIT, thread_reqd, cols_to_sort, asc, THREAD_COUNT, INPUT_FILE, OUTPUT_FILE

    MEMORY_LIMIT = int(args[3])*1000*1000                   #initialise memory limit from user custom preference

    try:
        THREAD_COUNT = int(args[4])
        thread_reqd = True
    except:
        thread_reqd = False

    start = 4                                               #points to arg number which corresponds to asc/desc
    if(thread_reqd == True):
        start = 5
        
    asc = True if args[start].lower() == 'asc' else False               #asc order or desc order
    for i in range(start+1, len(args)):                                 #which all cols need to be sorted, add them to a list
        cols_to_sort.append(args[i])

    INPUT_FILE = args[1]
    OUTPUT_FILE = args[2]


# read the metadata file and initialise global variables
def read_metadata():
    print('[READING] Metadata')
    global TUPLE_SIZE, col_details

    with open('metadata.txt') as f:
        index = 0
        for line in f:
            line = line.split(',')
            size = int(line[1].rstrip())                    #width of that col. values
            col_details[line[0]] = [index, size]        #store <col_name, [col_index, col_width]> in dict. e.g : <c1, [0,10]>
            TUPLE_SIZE += size                          #find the length of each record
            index += 1
    f.close()

    for i in cols_to_sort:                              #get the acutal indices of the cols that need to be sorted
        col_index_to_sort.append(col_details[i][0])


# calculates and sets various global variables related to tuples, sub-files based on input file data
def set_details():
    global TOTAL_TUPLES, TOTAL_FILESIZE, TOTAL_SUBFILES, TUPLES_PER_SUBFILE, ROW_LEN

    # Open the input file in read mode
    with open(INPUT_FILE) as f:
        # Iterate through each line of the file and keep track of the line count
        for i, l in enumerate(f):
            pass
    
    # Calculate the total number of tuples in the input file
    TOTAL_TUPLES = i + 1
    # Calculate the total size of the input file in bytes based on tuple size
    TOTAL_FILESIZE = TOTAL_TUPLES*TUPLE_SIZE
    # Calculate the total number of subfiles required  based on given MEMORY_LIMIT : ceil(TOTAL_FILESIZE / MEMORY_LIMIT)
    TOTAL_SUBFILES = (TOTAL_FILESIZE+MEMORY_LIMIT-1)//MEMORY_LIMIT
    # Calculate the number of tuples that can fit into each subfile in memory : floor(MEMORY_LIMIT / TUPLE_SIZE)
    TUPLES_PER_SUBFILE = MEMORY_LIMIT//TUPLE_SIZE

    # Open the input file again to determine the length of each record(including new line) in the input file
    with open(INPUT_FILE, 'r') as f:
        ROW_LEN = len(f.readline())+1
    f.close()

    #debug onto console
    print('[DETAILS] Memory Limit = ' + str(MEMORY_LIMIT) + ' B')
    print('[DETAILS] Tuple Size = ' + str(TUPLE_SIZE) + ' B')
    print('[DETAILS] Total tuples = ' + str(TOTAL_TUPLES))

    # Print total file size only if multithreading is not required
    if(thread_reqd == False):
        print('[DETAILS] Total File Size = ' + str(TOTAL_FILESIZE) + ' B')
    print('[DETAILS] Sub Files Required = ' + str(TOTAL_SUBFILES))

    # if(TOTAL_FILESIZE < MEMORY_LIMIT):
        # print('[ERROR] Two Phase Merge Sort is not required as memory limit is more than file size')
        # sys.exit(1)

    # Check if the total size of subfiles exceeds the available memory limit ; during k-way merge sort, we get 1 tuple per sub-file into heap
    if(TOTAL_SUBFILES * TUPLE_SIZE > MEMORY_LIMIT):
         # Print an error message and exit the program if memory limit is insufficient
        print('[ERROR] Memory Limit is less than what is required to hold the tuples during second phase')
        sys.exit(1)


#given a record line in input file, extract each attribut_value in that record and return a list of those attribute_values
def line_to_tuple(line):
    res = []
    for i in col_details.values():
        res.append(line[:i[1]])
        line = line[i[1]+2:]
    return res


#create sub-files with (TUPLES_PER_SUBFILE) num of records in each sub-file from input file
#each subfile is named as : 0.txt, 1.txt, 2.txt,.......
def create_subfiles():
    print('[CREATING] Subfiles')
    count = 0
    filenum = 0
    f = open(str(filenum)+'.txt', 'w')
    f.close()

    with open(INPUT_FILE) as input:
        for line in input:
            if(count == 0):
                f = open(str(filenum)+'.txt', 'w')
                filenum += 1
            f.write(line)                           #write each record to curr sub-file
            count += 1
            if(count == TUPLES_PER_SUBFILE):        #if curr sub-file reaches it's max capacity => create new sub-file
                count = 0
                f.close()
                
        if(f.closed == False):
            f.close()

    input.close()


#sort all sub-files individually based on the col_index_to_sort ; during phase-1
def sort_subfiles():
    for i in range(TOTAL_SUBFILES):
        cur = str(i)+'.txt'                 # Get the current subfile name
        table= []                           # list to store the tuples from the subfile
        with open(cur, 'r+') as f:
            print('[SORTING] Subfile #' + str(i))
            for line in f:
                row = line_to_tuple(line)       # convert the record to a list of it's attributes
                table.append(row)           

            # Sort the list of tuples based on the specified columns (col_index_to_sort) : use in-built sort and itemgetter
            # The sorting can be in ascending or descending order based on the 'asc' variable
            if(asc == True):
                table.sort(key = itemgetter(*col_index_to_sort))
            else:
                table.sort(key = itemgetter(*col_index_to_sort), reverse=True)

            f.truncate(0)                       #resize the sub-file to 0 bytes : empty the file but don't delete it
            f.seek(0)                           #place the r/w head at the start of the file
            print('[WRITING] Subfile #' + str(i))
            
            # Write the sorted tuples back to the subfile
            for row in table:                   
                for cell in range(len(row)):
                    f.write(row[cell])
                    if(cell != len(row)-1):
                        f.write('  ')
                    else:
                        f.write(' ')
                f.write('\n')
            f.close()


#given a start,end line numbers to input file and sub-file_num ; do phase 1(create a sub-file and sort it) on it
#let a thread handle it parallely
def thread_handler(start, end, filenum):
    #step-1 : create the req. sub-file
    print('[CREATING] Subfiles #' + str(filenum))
    count = 0
    f = open(str(filenum)+'.txt', 'w')          #create the sub-file_num.txt file
    start_read = False

    with open(INPUT_FILE) as input:             #open the input file
        for line in input:
            if(count == start):                 #if curr_line_num = start value => we need to process from here
                start_read = True
            if(count == end):                   #if curr_line_num = end value => we need to process till here
                f.write(line)
                f.close()
                break
            if(start_read):                     #add this line to curr sub-file
                f.write(line)
            count += 1
                
        if(f.closed == False):
            f.close()

    input.close()

    #step-2 : sort the req. sub-file
    table = []                                          #list to store the records : need it for sorting
    with open(str(filenum) + '.txt', 'r+') as f:
        print('[SORTING] Subfile #' + str(filenum))
        for line in f:                                  #add the record's attribute values to the table list
            row = line_to_tuple(line)
            table.append(row)

        if(asc == True):                                        #sort the table list based on sorting order of cols
            table.sort(key = itemgetter(*col_index_to_sort))
        else:
            table.sort(key = itemgetter(*col_index_to_sort), reverse=True)

        f.truncate(0)                                   #resize the sub-file to 0 bytes : empty the file but don't delete it
        f.seek(0)                                       #place the r/w head at the start of the file
        print('[WRITING] Subfile #' + str(filenum))
        
        for row in table:                               # Write the sorted tuples back to the subfile
            for cell in range(len(row)):
                f.write(row[cell])
                if(cell != len(row)-1):
                    f.write('  ')
                else:
                    f.write(' ')
            f.write('\n')
        f.close()


#very similar to phase 1, we are just creating thread_count number of threads for each sub-file and doing what we did in phase1
def threaded_phase1():
    global TOTAL_SUBFILES, TUPLES_PER_THREAD, file_counter
    PARTITIONS = TOTAL_SUBFILES
    # to divide the work into multiple threads
    TOTAL_SUBFILES = TOTAL_SUBFILES * THREAD_COUNT              #bcoz of threads, further divide num of sub-files
    TUPLES_PER_THREAD = TUPLES_PER_SUBFILE // THREAD_COUNT      #num of tuples that need to be processed per thread 

    #for each original sub-file, create thread_count number of further sub-files and call thread_handler method with
    #appropriate (start, end, further_sub_file_number)
    for i in range(PARTITIONS):
        threads = []
        for j in range(THREAD_COUNT):
            start = i*TUPLES_PER_SUBFILE + j*TUPLES_PER_THREAD      #start_num of record for this thread
            if(start > TOTAL_TUPLES):
                TOTAL_SUBFILES = file_counter
                break
            end = i*TUPLES_PER_SUBFILE + (j+1)*TUPLES_PER_THREAD-1  #end_num of record for this thread
            if(j==THREAD_COUNT-1):
                end = ((i+1)*TUPLES_PER_SUBFILE)-1
            if(end > TOTAL_TUPLES):
                TOTAL_SUBFILES = file_counter+1
                end = TOTAL_TUPLES

            threads.append(threading.Thread(target=thread_handler, args=(start, end, file_counter)))    #just store those threads, not executing right now
            file_counter += 1

        if(TOTAL_SUBFILES * TUPLE_SIZE > MEMORY_LIMIT):
            print('[ERROR] Memory Limit is less than what is required to hold the tuples during second phase')
            sys.exit(1)

        for j in threads:               #start those threads now
            j.start()
        for j in threads:               #wait till all those threads complete their execution
            j.join()


#create sub-files from input file and sort those sub-files individually
def phase1():
    print('[STARTED] Phase 1')

    #if no multi-threading => simply create those sub-files and sort them
    if(thread_reqd == False):
        create_subfiles()
        sort_subfiles()
    else:               #else call threaded phase 1 function
        threaded_phase1()
        
    print('[COMPLETED] Phase 1 after ' + str(time.time() - start_time) )


# k-way merge step : we need to merge those sub-files back to 1 file(output.txt)
# insert 1st record of all sub-files into heap and pop and write to output.txt file one by one
# when we do a pop, add next record of that file into the heap
# do it till the heap becomes empty => we processed all sub-files
def phase2():
    print('[STARTED] Phase 2')
    filenames = [str(x)+'.txt' for x in range(TOTAL_SUBFILES)]
    fp = {filename: open(filename, 'r+') for filename in filenames}
    out = open(OUTPUT_FILE, 'w')

    heap = []                                               #heap 

    for i in fp:
        line = fp[i].readline()
        if(len(line) == 0):
            fp[i].close()
            del fp[i]
        else:
            heapq.heappush(heap, heap_object(line_to_tuple(line), i))   #insert into heap based on our custom comparator

    files_done = 0

    while(files_done < TOTAL_SUBFILES):
        obj = heapq.heappop(heap)
        row = obj.val
        filename = obj.filename

        for i in range(len(row)):
            out.write(row[i])
            if(i != len(row)-1):
                out.write('  ')
        out.write('\n')

        line = fp[filename].readline()
        if(len(line) == 0):
            files_done += 1
        else:
            heapq.heappush(heap, heap_object(line_to_tuple(line), filename))

    for i in fp:
        fp[i].close()
    out.close()

    print('[COMPLETED] Phase 2')



# delete all the sub-files created in phase 1
def del_subfiles():
    print('[DELETING] Subfiles')

    for i in range(TOTAL_SUBFILES):
        os.remove(os.getcwd()+'/'+str(i)+'.txt')



if __name__ == '__main__':
    
    check_args(sys.argv)            #done
    read_metadata()                 #done
    set_details()                   #done
    phase1()                        #done   => create sub-files and sort them individually
    phase2()                        #done   => now do k-way merge sort of those sub-files
    del_subfiles()                  #done

    print("--- %s seconds ---" % (time.time() - start_time))
