#!/usr/bin/python3.13

import re
import unidecode
import pathlib
import argparse
from pprint import pprint
# from rich import print
import pandas as pd
from datetime import datetime

DIR_TO_STORE_CP = 'vystup' # tento adresar si pred spustenim uprav podla svojich potrieb
FILE_TO_LOAD = 'vstup/cp_dump.txt' # tento subor si vytvor v adresari specifikovanom vyssie, tu prilep vsetky riadky z CP od prveho spoja az po posledny, uloz a az tak spusti skript

pd.options.display.max_columns = 11
pd.options.display.max_colwidth = 99
pd.options.display.width = 180

def read_file(file: pathlib.Path) -> str:
    with open(FILE_TO_LOAD, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f]
        lines = '|'.join(line for line in lines if line) # delimit fields with pipe, use only non-empty strings
        return re.sub(r'\|prida. do mojich spojen.', ' ', lines) # where to split records


def remove_rubish(line_str: str) -> list[str]:
    lines = []
    for line in re.split(r'\|Detaily spojenia|Vytlačiť|Zdieľať\(1\)|Zobraziť na mape|Pridať do Mojich spojení', line_str):
        if len(line) > 5:
            line_str = line.lstrip('|') # strip pipe symbol from second line onwards
            line_str = re.sub(r'^.+? (\w{2})', r'\g<1>', line_str, flags=re.I)
            line_str = re.sub(r'celkov.+vzdialenos. ?(\d+) ?km', r'\g<1>', line_str, flags=re.IGNORECASE)
            line_str = re.sub(r'celkov.+as. ?(\d+ ?min)\|÷', r'\g<1>', line_str, flags=re.IGNORECASE)
            line_str = re.sub(r'(bus ?\d+? ?\d+?.+?(?=\d?)(\|h)?\|[^\|]+)', r'a', line_str, flags=re.IGNORECASE)
            line_str = re.sub(r'\|detaily spojenia.+', r'', line_str, flags=re.IGNORECASE)
            line_str = line_str.replace(',,', ' ')
            line_str = line_str.replace('spojenia ↓|', '')
            # line_str = re.sub(r'spojenia.+? (\D{2})', r'\1', line_str, flags=re.IGNORECASE)
            # line_str = [re.sub(r'\w+? \d+\|.+?(\d{1,2}:\d{2})', r'\1', line,
                            # flags=re.I) for line in line_str if line]
            # line_str = re.sub(r'(\d+ .+? \S+?\|)', r'', line_str, flags=re.IGNORECASE)
            # line_str = re.sub(r'\|Pridať do Mojich spojení', r'', line_str, flags=re.IGNORECASE)
            if 'Železničná spoločnosť Slovensko' in line_str:
                ls = line_str.split('|')
                [den, dlzka, *l, cas_o, miesto_o, cas_p, miesto_p] = ls
                dlzka = re.sub(r'celkov.+.+as. ?(\d+ ?min)', r'\g<1>', dlzka, flags=re.IGNORECASE)
                line_str = '|'.join([den, dlzka, 'v', cas_o, miesto_o, cas_p, miesto_p])
                # print(line_str)
            # print(line_str)
            lines.append(line_str)
    return lines

# print(read_file())
# lines = [x.split('|') for x in remove_rubish(read_file())]
# print(lines)


def process_records_to_final_table(file_to_read: str,
        col_names: list = 'den dlzka dp c_o m_o c_p m_p'.split()) -> pd.DataFrame:
    '''spravuje riadky vstupu a zabezpeci vystup, kde v pripade viacerych typov
    dopravneho prostriedku zobrazi tento stlpec'''
    # km - vzdialenost v km
    # dp - typ dopravneho prostriedku
    # c_o - cas odchodu
    # c_p - cas prichodu
    # m_o - miesto odchodu
    # m_p - miesto prichodu
    lines = [x.split('|') for x in remove_rubish(read_file(file_to_read))]
    # print(lines)
    series_list = []
    for line in lines:
        try:
            series = pd.Series(line, index=col_names)
        except ValueError as e:
            print(e, line)
        else:
            series_list.append(series)
        df = pd.concat(series_list, axis=1).T.reset_index(drop=True)
    # df = pd.DataFrame(lines, columns=col_names)
    dp = df.loc[:, 'dp'].values # ziska vsetky hodnoty pre typ dopravneho prostriedku
    SHOW_DP = False if len(set(dp)) < 1 else True # ak je viac typov dopravneho prostriedku, pouzi neskor pre jeho zobrazenie
    print(f'{SHOW_DP=}')
    df['time'] = pd.to_datetime(df.c_o, format='%H:%M').dt.time # ziska datetime objekt, ktory posluzi na spolahlive zoradenie potencialne premiesanych hodnot v tabulke
    df.set_index('time', inplace=True) # pouzije tento stlpec ako index tabulky
    df.sort_index(inplace=True) # prevedie samotne zotriedenie podla hodnot indexu
    if SHOW_DP: # tu sa pouzije FLAG, ktory urci, ci ponechat zobrazenie DP
        df = df.iloc[:, [2,3,4,5,6,1,0]]
    else:
        df = df.iloc[:, [3,4,5,6,1,0]]
    return df


def get_ascii_name(names: list) -> str:
    names_set = set(unidecode.unidecode(name) for name in names) # odstran diakritiku pre lepsiu prehladnost
    names_list = sorted(names_set, key= lambda x: len(x)) # zorad z mnoziny zastavok
    print('names_list: ', names_list)
    return names_list[0].lower().replace(' ', '_') if names_list else 'nezname' # nechaj nazov tej najkratsej


def get_current_year():
    return datetime.now().year


def get_filename_to_store_df(df: pd.DataFrame) -> pathlib.Path:
    m_o = get_ascii_name(df.m_o.values)
    m_p = get_ascii_name(df.m_p.values)
    year = get_current_year()
    print(f'{m_o}_{m_p}')
    return pathlib.Path(DIR_TO_STORE_CP) / f'cestovny_poriadok_{m_o}_{m_p}_{year}.csv'


def save_all_dfs(file_to_read=FILE_TO_LOAD):
    df = process_records_to_final_table(file_to_read)
    pprint(df)
    filename_to_store = get_filename_to_store_df(df)
    df.to_csv(filename_to_store, sep='|', index=False) # uloz dataframe, nezapisuj index, ten sluzil len na pravne zoradenie - tu vznika novy subor, pripadne prepise existujuci
    MIXED_WEEKEND = False if len(set(df.den.values)) == 1 else True # ak vo vstupnom subore bolo viacero dni v tyzdni, pouzivane hlavne pri vikendoch, nastav FLAG, aby sa vykonalo porovnanie medzi sobotou a nedelou
    print(df.columns)
    print(f'{MIXED_WEEKEND=}')
    year = get_current_year()
    if MIXED_WEEKEND:
        from collections import defaultdict
        day_dict = defaultdict(list) # nastav defaultdict, ktoreho hodnoty sa inicializuju na prazdny list a hned sa aj prilepi hodnota - den v tyzdni
        df_list = df.values.tolist()
        for r in df_list:
            # print(f'{r=}')
            day_dict[(r[0], r[1], r[2], r[3], r[4], r[5])].append(r[-1])
        day_list = [k + (','.join(sorted(v, key=lambda x: x[0], reverse=True)),) for k, v in day_dict.items()]
        # print('day list ---', day_list)
        df_weekend = pd.DataFrame.from_records(day_list, columns='dp c_o m_o c_p m_p dlzka den'.split())
        df_weekend.to_csv(str(filename_to_store).replace(f'_{year}', f'_wknd_{year}'),
                          sep='|', index=False)  # tu vznika novy subor, kde najdes vystup skriptu pre vikendy
        pprint(df_weekend)

# save_all_dfs(file_to_read=FILE_TO_LOAD)

# perl -i.$SECONDS -wple 's/liberationsans/Linux Libertine/ig && s/\d+\.\d+px/12px/' cestovny_poriadok_halic_lucenec_2022_december.svg

