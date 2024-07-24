from utils.help import Help
import psycopg2.extras

def extract_fn():
    try:
        pg = Help.postgre_connect()
        cur =pg.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cur.execute('DROP TABLE IF EXISTS employee')
        
        create_script = ''' CREATE TABLE IF NOT EXISTS employee (
            id int PRIMARY KEY,
            name varchar(40) NOT NULL,
            salary int,
            dept varchar(40)
            )'''
        cur.execute(create_script)
        
        insert_script = 'INSERT INTO employee(id,name,salary,dept) VALUES(%s,%s,%s,%s)'
        insert_values = [(1,"eswanth", 25000, "D1"),(2,"ranjith", 26000, "D2"),(3,"sathish", 30000, "D3")]
        
        for values in insert_values:
            cur.execute(insert_script,values)
            
        update_script = 'UPDATE employee SET salary = salary + (salary * 0.5)'
        cur.execute(update_script)
        
        delete_script = 'DELETE FROM employee WHERE name = %s'
        del_record = ("sathish",)
        cur.execute(delete_script,del_record)
            
        cur.execute('SELECT * FROM employee')
        for record in cur.fetchall():
            print(record['name'], record['salary'])
        
        pg.commit()
        
        
    except Exception as error:
        print(error)
        
    finally:
        pg.close()
        cur.close()