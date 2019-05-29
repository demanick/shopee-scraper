import os
import pandas as pd
import re
import warnings

from sys import argv

# suppress warnings
warnings.filterwarnings('ignore')
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


# Row-wise functions to be applied to pandas dataframe
def new_name(row):
    for i, v in enumerate(row[-7:]):
        if pd.isnull(v):
            row[1] = ','.join([str(x) for x in row[1:2+i]])
            row[2:17] = row[2+i:17+i]
            break
        elif i == 6:
            i += 1
            row[1] = ','.join([str(x) for x in row[1:2+i]])
            row[2:17] = row[2+i:17+i]
    return row


def row_replace(row):
    if len(row[1]) != 3:
        new_name = ''.join(row[1][:-2])
        new_row = [new_name]
        new_row.extend(row[1][-2:])
        row[1] = new_row
    new_rows = row[1].copy()
    for i, v in enumerate(new_rows):
        row[1 + i] = v
    return row


# comprehensive cleaning function for dataset
def clean_dataset(file):
    # load data
    products_df = pd.read_csv(file)

    # 1st error type: too many columns of data
    bad_rows1 = products_df[~products_df.iloc[:, -7:].isna().values.all(axis=1)]
    products_df[~products_df.iloc[:, -7:].isna().values.all(axis=1)] = bad_rows1.apply(new_name, axis=1).values
    products_df = products_df.drop(columns=products_df.columns[-7:])

    # 2nd error type: too few columns of data
    bad_rows2 = products_df[products_df.catid == 'main']
    bad_rows2.name = bad_rows2.name.str.replace('"', '').str.split(',').tolist()
    bad_rows2.iloc[:, 4:] = bad_rows2.iloc[:, 2:-2].values
    products_df[products_df.catid == 'main'] = bad_rows2.apply(row_replace, axis=1).values

    # 3rd error type: data split across multiple lines
    products_df.id = products_df.astype(str)
    bad_rows3 = products_df[~products_df.id.str.match(r'^[\d]+$')].index.values
    for row_num in bad_rows3:
        if row_num - 1 in bad_rows3:
            continue
        else:
            target_num = row_num-1
            target_row = products_df.iloc[target_num, :]
            data_rows = []
            while row_num in bad_rows3:
                data_rows.append(row_num)
                row_num += 1
            name_iterable = products_df.iloc[data_rows, 0].tolist()
            name_iterable.insert(0, target_row[1])
            target_row[1] = ','.join(name_iterable)
            target_row[2:17] = products_df.iloc[data_rows[-1], :][1:16]
            products_df.iloc[target_num, :] = target_row
    products_df = products_df.drop(index=bad_rows3)

    # write to new csv
    new_file_path = os.path.splitext(file)[0] + '_clean.csv'
    products_df.to_csv(new_file_path)

    # print rows that still have issues
    bad_vals = []
    for v in products_df.catid.unique():
        if type(v) == float:
            continue
        if re.fullmatch(r'^[\d]+$', v) is None:
            bad_vals.append(v)
    bad_rows4 = products_df[products_df.catid.isin(bad_vals)]
    print('Bad values at: ', bad_rows4.index.tolist())


if __name__ == '__main__':
    file_name = argv[1]
    file_path = os.path.join(ROOT, 'data', file_name)
    clean_dataset(file_path)
