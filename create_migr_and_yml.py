# для запуска требуется:
# 1) установить пакет https://pypi.org/project/ddlparse/ pip install ddlparse
# 2) указать пути source_path (путь к ddl) и target_path (сюда генерятся скрипты и yml)
# 3) указать номер первой миграции (далее для каждого ddl файла автоинкрементом увеличится номер)
# 4) после выполнения скрипта проверить вывод в консоль - если были обнаружены пользовательские типы данных, то скрипт напишет соотв. столбец
# 5) указать datebase и schema для yml файлов

from ddlparse import DdlParse
import datetime
from pathlib import Path

types_match = {'UUID': 'String()', 'VARCHAR': 'String()', 'INT4': 'Int32()','INT2':'Int16()', 'INT8': 'Int64()',
               'FLOAT4': 'Float32()', 'FLOAT8': 'Float64()','BOOL': 'Int8()', 'TIMESTAMP': 'Datetime()',
               'TIMESTAMPTZ': 'Datetime()', 'JSON': 'String()', 'JSONB': 'String()','TEXT':'String()'}

source_path = '/home/konstantin/files/ddl/'
migr_target_path = '/home/konstantin/files/final_migr_files/'
yml_target_path = '/home/konstantin/files/final_yml_files/'

def create_migr_files():
    global source_path
    global migr_target_path
    incr_field = 'created_at' # increment filed in clickhouse
    startmigr = 7 # first migration number
    for source_filename in Path(source_path).glob('*.sql'):
        migr_str = ''''''
        yml_str='''
        database: {}
        schema: {}
        table: {}
        type_load: incr
        col_incr: updated_at
        pk: {}
        source:
          schema: {}
          table: {}
        target:
          schema: stg
          table: {}
        map:  
        '''
        tablename = source_filename.stem
        with open(str(source_filename),'r+') as source_file, \
             open(migr_target_path+'{}_init.py'.format(startmigr+1),"w+") as migr_target_file, \
             open(source_path+'template.py','r+') as template:
            table = DdlParse().parse(ddl=source_file.read(), source_database=DdlParse.DATABASE.postgresql)
            #start fill datatypes for migration
            for i in table.columns.values():
                field_name = i.name
                canbenull = not i.not_null
                if i.primary_key == True:
                    pk_field = i.name
                if i.data_type in types_match:
                    field_type = types_match[i.data_type]
                else:
                    field_type = 'String()'
                    print(i.data_type+' заменили на строку в столбце '+i.name)
                migr_str += "sa.Column('{}', clickhouse_sqlalchemy.types.common.{}, nullable={}),\r\n\t\t".format(field_name,field_type,canbenull)
            migr_str += "sa.PrimaryKeyConstraint('{}'),\r\n\t\t".format(pk_field)
            migr_str += "engines.MergeTree(partition_by=sa.text('toYYYYMM({})'), order_by=('{}'))".format(incr_field,incr_field)
            #end fill datatypes for migration

            #start generate {}_init.py files
            script_text = template.read()
            script_text = script_text.replace('rev_id',str(startmigr))
            script_text = script_text.replace('prev_revision_id', str(startmigr-1))
            script_text = script_text.replace('rev_datetime', str(datetime.datetime.now()))
            script_text = script_text.replace('table_name', tablename)
            script_text = script_text.replace('column_srings', migr_str)
            migr_target_file.write(script_text)
            startmigr+=1
            #end generate {}_init.py files



def create_yml_files():
    global source_path
    global yml_target_path

    for source_filename in Path(source_path).glob('*.sql'):
        yml_str=\
'''database: {}
schema: {}
table: {}
type_load: incr
col_incr: updated_at
pk: {}
source:
\tschema: {}
\ttable: {}
target:
\tschema: stg
\ttable: {}
map:'''
        tablename = source_filename.stem

        database = 'testing'
        pk_field = ''
        with open(yml_target_path + '{}.yml'.format(tablename), "w+") as yml_target_file,\
        open(str(source_filename),'r+') as source_file:
            table = DdlParse().parse(ddl=source_file.read(), source_database=DdlParse.DATABASE.postgresql)
            for i in table.columns.values():
                if i.primary_key == True:
                    pk_field = i.name
                if i.data_type != 'BOOL':
                    yml_str += "\r\n\t-\r\n\t\tsource: {}\r\n\t\ttarget: {}".format(i.name,i.name)
                else:
                    yml_str += "\r\n\t-\r\n\t\tsource: {}\r\n\t\ttarget: {}".format("case when {} = 'false' then 0 when {} = 'true' then 1 end".format(i.name,i.name), i.name)

            yml_str = yml_str.format(database, table.schema, table.name, pk_field, table.schema, table.name, tablename)
            yml_target_file.write(yml_str)
            #print(yml_str)

if __name__ == '__main__':
    create_migr_files()
    create_yml_files()
