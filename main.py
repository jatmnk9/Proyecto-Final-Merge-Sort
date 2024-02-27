import sqlite3 #gestión de la base de datos
import random 
import threading 
import time 
import os 

class Location:
    def __init__(self, id, distance_from_origin):
        self.id = id
        self.distance_from_origin = distance_from_origin

    def __lt__(self, other):
        return self.distance_from_origin < other.distance_from_origin

def create_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS clientes (
                        id INTEGER PRIMARY KEY,
                        distance_from_origin INTEGER)''')

def delete_table(cursor):
    cursor.execute('''DROP TABLE clientes''')

def insert_random_locations(cursor, num_locations):
    for i in range(num_locations):
        distance_from_origin = random.randint(0, 100)
        cursor.execute("INSERT INTO clientes (id, distance_from_origin) VALUES (?, ?)", (i, distance_from_origin))

def load_locations_from_database(cursor):
    cursor.execute("SELECT * FROM clientes")
    locations = []
    for row in cursor.fetchall():
        id, distance_from_origin = row
        locations.append(Location(id, distance_from_origin))
    return locations

# number of elements
MAX = 5000000

# number of threads
THREAD_MAX = 8

def merge(arr, low, mid, high):
    left = arr[low:mid+1]
    right = arr[mid+1:high+1]
 
    n1 = len(left)
    n2 = len(right)
    i = j = 0
    k = low
 
    while i < n1 and j < n2:
        if left[i] < right[j]:  
            arr[k] = left[i]
            i += 1
        else:
            arr[k] = right[j]
            j += 1
        k += 1
 
    while i < n1:
        arr[k] = left[i]
        i += 1
        k += 1
 
    while j < n2:
        arr[k] = right[j]
        j += 1
        k += 1


# Lock para garantizar la exclusión mutua
lock = threading.Lock()

# merge sort function
def merge_sort(arr, low, high):
    #Base case
    if low < high:
        mid = low + (high - low) // 2
 
        merge_sort(arr, low, mid)
        merge_sort(arr, mid + 1, high)
 
        merge(arr, low, mid, high)

# thread function for multi-threading
def merge_sort_threaded(arr, low, high):
    threads = []
    section_size = MAX // THREAD_MAX
    
    for i in range(THREAD_MAX):
        start_index = ((THREAD_MAX + i) * section_size)
        end_index = ((THREAD_MAX + i + 1) * section_size) - 1
        t = threading.Thread(target=merge_sort, args=(arr, start_index, end_index))
        t.start()
        threads.append(t)
    
    # Esperar a que todos los hilos secundarios completen su trabajo
    for t in threads:
        t.join()


# Save sorted list to a text file
def save_sorted_list_to_file(sorted_list):
    with open("ordenamiento.txt", "w") as file:
        for item in sorted_list:
            file.write(f"Client {item.id}: Distance from origin: {item.distance_from_origin}\n")


#main
if __name__ == "__main__":
    # Conectar a la base de datos (creará una nueva si no existe)
    db_connection = sqlite3.connect('clientes.db')
    cursor = db_connection.cursor()
    #delete_table(cursor)
    # Crear la tabla si no existe
    create_table(cursor)
    # Insertar ubicaciones aleatorias de clientes
    num_locations = MAX
    insert_random_locations(cursor, num_locations)
    db_connection.commit()
    print("Customer locations inserted into database.")

    # Cargar las ubicaciones de los clientes desde la base de datos
    a = load_locations_from_database(cursor)
    print("Customer locations loaded from database.")

    # Copia del arreglo para usar en merge sort secuencial
    arr_seq = [loc for loc in a]
 
    # t1 and t2 for calculating time for
    # merge sort multithreaded
    t1 = time.perf_counter()
    merge_sort_threaded(arr_seq, 0, len(arr_seq)-1)
    t2 = time.perf_counter()
    timeParallelism= t2-t1
    print(f"Time taken for multithreaded merge sort: {t2 - t1:.6f} seconds")

    # t3 and t4 for calculating time for
    # merge sort sequential
    t3 = time.perf_counter()
    merge_sort(arr_seq, 0, len(arr_seq)-1)
    t4 = time.perf_counter()
    timeSequential= t4-t3
    print(f"Time taken for sequential merge sort: {t4 - t3:.6f} seconds")

    idealParallelism = (((t4-t3)/(t2-t1))-1)*100

    print("The algorithm is ",idealParallelism,"%","faster.")

    speedup = timeSequential/timeParallelism
    print("SpeedUp: ", speedup)

    # Hay 4 procesadores
    eficiencia = (speedup/ 4)*100
    print(f"The algorithm is {eficiencia:.3f}% efficient.")

    # Verificar si el archivo existe antes de intentar eliminarlo
    if os.path.exists("ordenamiento.txt"):
        os.remove("ordenamiento.txt")
    
    lock.acquire()
    save_sorted_list_to_file(arr_seq)
    lock.release()

    # Cerrar la conexión con la base de datos
    cursor.close()
    db_connection.close()
