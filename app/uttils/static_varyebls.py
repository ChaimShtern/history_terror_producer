relevant_fields = [
    # זמנים ותאריכים
    'iyear', 'imonth', 'iday',

    # מיקום גיאוגרפי
    'latitude', 'longitude', 'country_txt', 'region_txt', 'city',

    # מידע על קבוצות תוקפות
    'gname', 'guncertain1', 'nperps', 'gname2', 'gname3',

    # מאפייני התקיפה
    'attacktype1_txt', 'attacktype2_txt', 'attacktype3_txt',
    'targtype1_txt', 'target1', 'natlty1_txt',

    # נתוני נשק
    'weaptype1_txt', 'weapsubtype1_txt',
    'weaptype2_txt', 'weapsubtype2_txt',
    'weaptype3_txt', 'weapsubtype3_txt',
    'weaptype4_txt', 'weapsubtype4_txt',

    # נפגעים ונזק
    'nkill', 'nwound', 'nkillter',
    'property', 'propvalue', 'propextent_txt',

    # חטיפות וסחיטה
    'ishostkid', 'nhostkid', 'ransom', 'ransomamt', 'hostkidoutcome_txt',

    # הערות ונתוני מקור
    'summary', 'addnotes', 'scite1', 'scite2', 'scite3'
]

relevant_neo4j_fields = ["gname", 'country_txt','region_txt','attacktype1_txt','targtype1_txt']

relevant_elastic = ['country_txt', 'region_txt', 'city','iyear', 'imonth', 'iday','summary']