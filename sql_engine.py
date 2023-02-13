import pandas as pd
import csv

# Function to split the query into tokens
# in the form of a list
def split_query(q):
    # print(q)
    if " NATURAL JOIN" in q:
        q = q.replace(' NATURAL JOIN', '')
        jointype = 0
    else: jointype = 1
    print(q)
    tokens = q.split()
    print(tokens)
    

    select_indices = []
    from_indices = []
    where_indices = []


    if(tokens[0] != 'SELECT'):
        print("First word should be SELECT")

    # print(list(enumerate(tokens)))
    # Check if the query is valid
    for indx, tkns in enumerate(tokens):
        if tkns == "SELECT":
            select_indices.append(indx)
    
    # print(select_indices)

    for indx, tkns in enumerate(tokens):
        if tkns == "FROM":
            from_indices.append(indx)
            # print(from_indices)

    for indx, tkns in enumerate(tokens):
        if tkns == "WHERE":
            where_indices.append(indx)
            # print(where_indices)

    if((len(select_indices)!=1) or (len(from_indices) != 1) or (len(where_indices) > 1)):
        print("Invalid Query 1")
    
    select_idx, from_idx = select_indices[0], from_indices[0]
    if len(where_indices) == 1:
        where_idx = where_indices[0]
    else: where_idx = None

    if(from_idx < select_idx ):
        print("Invalid Query 2")

    if(where_idx):
        if(where_idx < select_idx or where_idx < from_idx):
            print("Invalid Query 3")

    req_attributes = tokens[select_idx + 1 : from_idx]
    
    if(where_idx):
        req_tables = tokens[from_idx+1 : where_idx]
        req_conditions = tokens[where_idx+1 : ]
    else:
        req_tables = tokens[from_idx+1 : ]
        req_conditions = []
    # print(req_attributes)
    # print(req_tables)
    print(req_conditions)
    sql_select(req_attributes, req_tables, jointype, req_conditions)

def sql_select(req_atts, req_tabs, joinType,req_conditions):
    print(req_atts)
    print(req_tabs)
    print(joinType)
    print(len(req_tabs))
    df = []
    i = 0
    while i < len(req_tabs):
        df.append(pd.DataFrame(pd.read_csv(req_tabs[i])))
        i+=1

    # df.append(pd.DataFrame(pd.read_csv(req_tabs[0])))
    # df.append(pd.DataFrame(pd.read_csv(req_tabs[1])))

    if(len(req_atts) == 1):
        if(len(req_tabs) == 1):
            if(req_atts[0] == '*'):
                print(df[0])
            else :
                print(df[0][req_atts[0]])
        else:
            if(joinType):
                res_df = sql_from_cross(req_atts, df, req_tabs, req_conditions)
            else:
                res_df = sql_from_natural(req_atts, df, req_tabs, req_conditions)
    # print(df[req_atts[0]])
    elif (len(req_atts) > 1):
        if(len(req_tabs) == 1): 
            print(df[0][req_atts])
        else:
            if(joinType):
                res_df = sql_from_cross(req_atts, df, req_tabs, req_conditions)
            else:
                res_df = sql_from_natural(req_atts, df, req_tabs, req_conditions)
    print(res_df[req_atts])
def sql_from_cross(atts,dfs,tables, req_conditions):
    # cross product
    # Creating resulting dataframe

    file_string = []
    res_list = []
    new_list = []
    res_cols = []
    product = 1

    i = 0
    while i < len(tables) :
        file_string.append(tables[i].replace('csv', ''))
        i+=1
    i = 0
    while i < len(dfs):
        res_list.append(list(dfs[i].columns))
        i+=1

    i = 0
    while i < len(res_list):
        new_list.append([file_string[i] + x for x in res_list[i]])
        i+=1

    i = 0
    while i < len(new_list):
        res_cols+=new_list[i]
        i+=1
    
    i = 0
    while i < len(dfs):
        product *= dfs[i].shape[0]
        i+=1

    # string1 = tables[0].replace('csv', '')
    # string2 = tables[1].replace('csv', '')
    # res_list1 = list(dfs[0].columns)
    # res_list2 = list(dfs[1].columns)
    # new_list1 = [string1 + x for x in res_list1]
    # new_list2 = [string2 + x for x in res_list2]
    # res_df = pd.DataFrame(columns=res_cols, index=range(dfs[0].shape[0]*dfs[1].shape[0]))
    res_df = pd.DataFrame(columns=res_cols, index=range(product))

    # res_df=pd.DataFrame(columns=list(dfs[0].columns)+list(dfs[1].columns),index=range(dfs[0].shape[0]*dfs[1].shape[0]))
    k = 0
    for i in range (0, len(dfs[0])):
        for j in range (0, len(dfs[1])):
            res_df.loc[k]=list(dfs[0].loc[i])+list(dfs[1].loc[j])
            k+=1
    # print(res_df.columns)
    # print(res_df)
    # print(res_df[atts])
    res_df_from = from_where(res_df, req_conditions)
    return res_df_from
    # print(res_df_from)
def sql_from_natural(atts,dfs,tables, req_conditions):
    set1 = set(list(dfs[0].columns))
    set2 = set(list(dfs[1].columns))
    common_attr = set1 & set2
    set2 = set2 - common_attr
    lst2 = list(set2)
    # res_cols = set1.union(set2)
    res_cols = list(dfs[0].columns) + lst2
    print(res_cols)
    res_df = pd.DataFrame(columns=(res_cols), index = range(dfs[0].shape[0]*dfs[1].shape[0]))
    print(res_df.columns)
    # common_attr = list(common_attr)
    print(common_attr)

    k = 0
    for i in range (0, len(dfs[0])):
        for j in range (0, len(dfs[1])):
            if(set(dfs[0].loc[i,list(common_attr)]) == set(dfs[1].loc[j,list(common_attr)])):
                res_df.loc[k]=list(dfs[0].loc[i])+list(dfs[1].loc[j, list(set(dfs[1].columns) - common_attr)])
                k+=1
    res_df = res_df.dropna()
    # print(res_df)
    res_df_from = from_where(res_df, req_conditions)
    return res_df_from
    # print(res_df_from)


def from_where(res_df, conditions):
    op = conditions[1]
    cond = conditions[2]
    try:
        cond = eval(cond)
    except: cond = cond.replace("'", "")
    # if isinstance(cond, float) or isinstance(cond, int):
    #     cond = eval(cond)
    print(cond)
    if op == '=':
        res_df = res_df[res_df[conditions[0]] == cond]
    elif op == '!=':
        res_df = res_df[res_df[conditions[0]] != cond]
    elif op == '<=':
        res_df = res_df[res_df[conditions[0]] <= cond]
    elif op == '>=':
        res_df = res_df[res_df[conditions[0]] >= cond]
    elif op == '>':
        res_df = res_df[res_df[conditions[0]] > cond]
    elif op == '<':
        res_df = res_df[res_df[conditions[0]] < cond]

    return res_df
    # print(res_df[res_df['name'] == 'Velociraptor'])
    # print(res_df)


query = "SELECT attr1,attr2,attr3 FROM dino1.csv,dino2.csv WHERE condition1,condition2"
select_query_one_attribute = "SELECT NAME FROM dino1.csv"
select_query_star = "SELECT * FROM dino1.csv"
select_query_multi_attribute = "SELECT dino1.diet,dino2.stance FROM dino1.csv,dino2.csv"
select_query_natural_join = "SELECT diet,stance FROM dino1.csv NATURAL JOIN dino2.csv WHERE name = Euoplocephalus'"
select_query_multi_attribute = select_query_multi_attribute.replace(',', ' ')
query = query.replace(',', ' ')
select_query_natural_join = select_query_natural_join.replace(',', ' ')



split_query(select_query_natural_join)

