#!/usr/bin/env python3
import pprint
import collections
from Chinook_Python import *

row_count = 0

def project(relation, columns):
    global row_count
    myset=set()
    result =set([tuple(getattr(row, column) for column in columns)
        for row in relation])
    Result= collections.namedtuple('Result',columns,rename= True)
    for r in result:
        myset.add(Result._make(r))
    row_count += len(myset)
    return myset

def select(relation, predicate):
    global row_count
    result=[]
    for r in relation:
        if(predicate(r)):
            result.append(r)
    row_count += len(result)
    return set(result)


def rename(relation, new_columns=None, new_relation=None):
    global row_count
    myset=set()
    if new_columns is None and new_relation is None:
        return relation
    New_Rel=collections.namedtuple('New_Rel',new_columns,rename= True)
    for r in relation:
        myset.add(New_Rel._make(r))
    row_count += len(myset)
    return myset


def cross(relation1, relation2):
    global row_count
    result=[]
    myset=set()
    r1 = next(iter(relation1))
    r2 = next(iter(relation2))
    attr = r1._fields + r2._fields
    Result= collections.namedtuple('Result',attr,rename= True)
    for r in relation1:
        for row in relation2:
            result.append(tuple([item for item in r] + [item for item in row]))
    for r in result:
        myset.add(Result._make(r))

    row_count += len(myset)
    return myset
   


def theta_join(relation1, relation2, predicate):
    global row_count
    result=[]
    myset=set()
    r1 = next(iter(relation1))
    r2 = next(iter(relation2))
    attr = r1._fields + r2._fields
    Result= collections.namedtuple('Result',attr,rename= True)
    for r in relation1:
        for row in relation2:
            if(predicate(r,row)):
                result.append(tuple([item for item in r] + [item for item in row]))
    for r in result:
        myset.add(Result._make(r))
    row_count += len(myset)
    return myset
   

def natural_join(relation1,relation2):
    global row_count
    results = []
    myset = set()
    r1 = next(iter(relation1))
    r2 = next(iter(relation2))

    t1atts = r1._fields
    t2atts = r2._fields
    t1columns = set(t1atts)
    t2columns = set(t2atts)
    m1map = {k: i for i, k in enumerate(t1atts)}
    m2map = {k: i for i, k in enumerate(t2atts)}

    join_on = t1columns & t2columns
    diff = t2columns - join_on
    list_attr = list(t1atts)
    list_diff = list(diff)
    for item in list_diff:
        list_attr.append(item)
    new_named_attrs = tuple(list_attr)
    Result= collections.namedtuple('Result',new_named_attrs,rename= True)
    def Is_match(row1, row2):
        return all(row1[m1map[rn]] == row2[m2map[rn]] for rn in join_on)

    for row1 in relation1:
        for row2 in relation2:
            if Is_match(row1, row2):
                row = row1[:]
                new_row = list(row)
                for rn in diff:
                    new_row.append(row2[m2map[rn]])
                results.append(new_row)
    for r in results:
        myset.add(Result._make(r))
    row_count += len(myset)
    return myset



# Query 1
print("***************** Query 1 *****************\n")

print("----------Select after Theta Join -------------\n")
pprint.pprint(
    project(
        select(
            theta_join(
                Album,
                rename(Artist, ['Id', 'Name']),
                lambda t1, t2: t1.ArtistId == t2.Id
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'
        ),
        ['Title']
    )
)

print("Number of tuples processed :%i\n"% (row_count))
row_count = 0

print("----------------- Cross Join -----------------\n")
pprint.pprint(
    project(
        select(
            select(
                cross(
                    Album,
                    rename(Artist, ['Id', 'Name']),
                ),
                lambda t: t.ArtistId == t.Id
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'
        ),
        ['Title']
    )
)

print("Number of tuples processed :%i\n"% (row_count))
row_count = 0

print("---------- Select before Theta Join ----------\n")
pprint.pprint(
    project(
        theta_join(
            Album,
            rename(
                select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers'),
                ['Id', 'Name']
            ),
            lambda t1, t2: t1.ArtistId == t2.Id
        ),
        ['Title']
    )
)

print("Number of tuples processed :%i\n"% (row_count))
row_count = 0

print("--------------- Natural Join ----------------\n")
pprint.pprint(
    project(
        natural_join(
            Album,
            select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers')
        ),
        ['Title']
    )
)

print("Number of tuples processed :%i\n"% (row_count))
row_count = 0


# Query 5
print("\n\n****************   Query  5   ****************\n")

print("----------Select after Theta Join -------------\n")
pprint.pprint(
    project(
        theta_join(
            Employee,
            theta_join(
                Customer,
                rename(
                theta_join(
                    Invoice,
                    rename(
                    theta_join(
                        InvoiceLine,
                        rename(
                            select(
                            theta_join(
                                Track,
                                rename(MediaType, ['MId', 'MName']),
                                lambda t1, t2: t1.MediaTypeId == t2.MId
                            ),
                            lambda t: t.MName == 'Purchased AAC audio file'
                            ),
                            ['Tid', 'Name', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes', 'UnitPrice','Mid','Mname']
                         ),
                        lambda t1, t2: t1.TrackId == t2.Tid
                    ),
                    ['InvoiceLineId', 'InId', 'TrackId','IUnitPrice', 'Quantity','Tid', 'Name', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes', 'UnitPrice','Mid','Mname']
                    ),
                    lambda t1,t2: t1.InvoiceId == t2.InId
                ),
                ['InvoiceId', 'CId', 'InvoiceDate', 'BillingAddress', 'BillingCity', 'BillingState', 'BillingCountry', 'BillingPostalCode', 'Total','InvoiceLineId', 'InvoiceId', 'TrackId','IUnitPrice', 'Quantity','Tid', 'Name', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes', 'UnitPrice','Mid','Mname']
                ),
                lambda t1,t2: t1.CustomerId == t2.CId
        ),
        lambda t1,t2: t1.EmployeeId == t2.SupportRepId
    ),
    ['FirstName', 'LastName']
    )
)

print("Number of tuples processed :%i\n"% (row_count))
row_count = 0

# Cross Join
print("----------------- Cross Join -----------------\n")
pprint.pprint(
    project(
        select(
            cross(
                Employee,
                select(
                    cross(
                        Customer,
                        rename(
                            select(
                            cross(
                                Invoice,
                                rename(
                                    select(
                                    cross(
                                        InvoiceLine,
                                        rename(
                                            select(
                                            select(
                                            cross(
                                                Track,
                                                rename(MediaType, ['Mid', 'Mname']),
                                            ),
                                            lambda t: t.MediaTypeId == t.Mid
                                             ),
                                            lambda t: t.Mname == 'Purchased AAC audio file'
                                             ),
                                            ['Tid', 'Name', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes', 'UnitPrice','Mid','Mname']
                                        )
                                    ),
                                    lambda t: t.TrackId == t.Tid
                                ),
                                ['InvoiceLineId', 'InId', 'TrackId','IUnitPrice', 'Quantity','Tid', 'Name', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes', 'UnitPrice','Mid','Mname']
                                )
                            ),
                            lambda t: t.InvoiceId == t.InId
                            ),
                            ['InvoiceId', 'CId', 'InvoiceDate', 'BillingAddress', 'BillingCity', 'BillingState', 'BillingCountry', 'BillingPostalCode', 'Total','InvoiceLineId', 'InvoiceId', 'TrackId','IUnitPrice', 'Quantity','Tid', 'Name', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes', 'UnitPrice','Mid','Mname']
                        )
                    ),
                    lambda t: t.CustomerId == t.CId
            )
            ),
            lambda t: t.EmployeeId == t.SupportRepId
        ),
    ['FirstName', 'LastName']
    )
)

print("Number of tuples processed :%i\n"% (row_count))
row_count = 0

print("---------- Select before Theta Join ----------\n")
pprint.pprint(
    project(
        theta_join(
            Employee,
                theta_join(
                Customer,
                rename(
                    theta_join(
                    Invoice,
                    rename(
                        theta_join(
                        InvoiceLine,
                        rename(
                            theta_join(
                            Track,
                            rename(
                                select(MediaType, lambda t: t.Name == 'Purchased AAC audio file'),
                                ['Mid','Mname']
                            ),
                            lambda t1, t2: t1.MediaTypeId == t2.Mid
                            ),
                            ['Tid', 'Name', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes', 'UnitPrice','Mid','Mname']
                        ),
                        lambda t1, t2: t1.TrackId == t2.Tid
                        ),
                        ['InvoiceLineId', 'InId', 'TrackId','IUnitPrice', 'Quantity','Tid', 'Name', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes', 'UnitPrice','Mid','Mname']
                    ),
                    lambda t1,t2: t1.InvoiceId == t2.InId
                    ),
                    ['InvoiceId', 'CId', 'InvoiceDate', 'BillingAddress', 'BillingCity', 'BillingState', 'BillingCountry', 'BillingPostalCode', 'Total','InvoiceLineId', 'InvoiceId', 'TrackId','IUnitPrice', 'Quantity','Tid', 'Name', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes', 'UnitPrice','Mid','Mname']
                ),
                lambda t1,t2: t1.CustomerId == t2.CId
        ),
        lambda t1,t2: t1.EmployeeId == t2.SupportRepId
    ),
    ['FirstName', 'LastName']     
    )
)
print("Number of tuples processed :%i\n"% (row_count))
row_count = 0

#Natural join
print("--------------- Natural Join ----------------\n")
pprint.pprint(    
    project(
        natural_join(
            Employee,
            natural_join(
                rename(Customer, ['CustomerId', 'FName', 'LName', 'Company', 'CAddress', 'Cust_City', 'Cust_State', 'Cust_Country', 'Cust_PostalCode', 'Cust_Phone', 'Cust_Fax', 'Cust_Email', 'EmployeeId']),
                natural_join(
                    Invoice,
                    natural_join(
                        InvoiceLine,
                        natural_join(
                            Track,
                            select(
                            rename(MediaType, ['MediaTypeId', 'MName']),
                            lambda t: t.MName == 'Purchased AAC audio file'
                            )
                        )   
                    )
                )
            )
        ),
        ['FirstName', 'LastName']
    )
)

print("Number of tuples processed :%i\n"% (row_count))
row_count=0

print("---------- Optimized Natural Join -----------\n")
pprint.pprint(
  project(
    natural_join(Employee,
      project(
        rename(
          project(
            natural_join(Customer,
              project(
                natural_join(Invoice,
                  project(
                    natural_join(InvoiceLine,
                      project(
                        natural_join(Track,
                          project(
                            select(MediaType, lambda t: t.Name == 'Purchased AAC audio file'),
                            ['MediaTypeId'],
                          )
                        ), ['TrackId']
                      )
                    ), ['InvoiceLineId', 'InvoiceId']
                  )
                ), ['InvoiceId', 'CustomerId']
              )
            ), ['CustomerId', 'SupportRepId']
          ), ['CustomerId', 'EmployeeId']
        ), ['EmployeeId']
      )
    ), ['FirstName', 'LastName']
  )
)

print("Number of tuples processed :%i\n"% (row_count))
row_count = 0
