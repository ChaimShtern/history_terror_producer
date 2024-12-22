from datetime import datetime
import pycountry


def get_country_region(country_name):
    # מציאת מידע על המדינה
    country = pycountry.countries.get(name=country_name)
    if not country:
        return None

    # האזור הגיאוגרפי של המדינה (חלק מ-ISO)
    subdivisions = pycountry.subdivisions.get(country_code=country.alpha_2)
    return subdivisions
def convert_dict_for_sql(data):
    # Parse the date
    try:
        date = datetime.strptime(data['Date'], '%d-%b-%y')
    except ValueError:
        date = None

    return {
        'iyear': date.year - 1000 if date else None,
        'imonth': date.month if date else None,
        'iday': date.day if date else None,
        'latitude': None,  # אין מידע על קווי רוחב ואורך
        'longitude': None,
        'country_txt': data.get('Country'),
        'region_txt': None,  # אין מידע על האזור
        'city': data.get('City'),
        'gname': data.get('Perpetrator', 'Unknown'),
        'guncertain1': 0 if data.get('Perpetrator', 'Unknown') != 'Unknown' else 1,
        'nperps': None,  # אין מידע על מספר התוקפים
        'gname2': None,
        'gname3': None,
        'attacktype1_txt': None,  # אין סוג תקיפה מוגדר
        'attacktype2_txt': None,
        'attacktype3_txt': None,
        'targtype1_txt': None,  # אין מידע על סוג המטרה
        'target1': None,
        'natlty1_txt': None,  # אין מידע על לאום
        'weaptype1_txt': data.get('Weapon'),
        'weapsubtype1_txt': None,
        'weaptype2_txt': None,
        'weapsubtype2_txt': None,
        'weaptype3_txt': None,
        'weapsubtype3_txt': None,
        'weaptype4_txt': None,
        'weapsubtype4_txt': None,
        'nkill': data.get('Fatalities', 0),
        'nwound': data.get('Injuries', 0),
        'nkillter': 0.0,  # מניחים שאין הרוגים בקרב תוקפים
        'property': 0,  # אין נזק לרכוש לפי הנתונים
        'propvalue': None,
        'propextent_txt': None,
        'ishostkid': 0,  # אין בני ערובה
        'nhostkid': None,
        'ransom': 0,  # אין כופר
        'ransomamt': None,
        'hostkidoutcome_txt': None,
        'summary': data.get('Description'),
        'addnotes': None,
        'scite1': None,
        'scite2': None,
        'scite3': None
    }


def convert_dict_for_neo4j(data):

    return {'gname': data['Perpetrator'],
            'country_txt': data['Country'],
            'region_txt':get_country_region(data['Country']) ,
            'attacktype1_txt': None,
            'targtype1_txt': None}
