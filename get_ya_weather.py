import requests
import csv

from typing import Generator


city_names={
    "Москва": "Moscow"
    , "Казань": "Kazan"
    , "Санкт-Петербург": "Saint-Petersburg"
    , "Тула": "Tula"
    , "Новосибирск": "Novosibirsk"
}


def get_json_weather_for_city() -> Generator:
    access_key = '' # YOUR API KEY
    headers = {
        'X-Yandex-Weather-Key': access_key
    }
    # словарь с городами и их широтой и долготой
    cities_dict={
        "moscow": {"lat":55.751244, "lon":37.618423}
        , "kazan": {"lat":55.796391, "lon":49.108891}
        , "saint_petersburg": {"lat":59.930995, "lon":30.360776}
        , "tula": {"lat":54.204838, "lon":37.618492}
        , "novosibirsk": {"lat":55.018803, "lon":82.933952}
    }
    # цикл по городам и получение json с прогнозом погоды
    for city in cities_dict.keys():
        lat=cities_dict.get(city, None).get("lat", None)
        lon=cities_dict.get(city, None).get("lon", None)
        url=f"https://api.weather.yandex.ru/v2/forecast?lat={lat}&lon={lon}"
        try:
            with requests.get(url, headers=headers) as response: # запрос через контекстный менеджер для автоматического закрытия сессии
                city_json=response.json()
                yield city_json
        except Exception as e:
            print(str(e))
            print(f"error in {city} city")



def parse_city_weather_days(city_json: Generator) -> dict:
    # словарь для итоговых данных по прогнозу
    forecast_dict= {
        "city":[]
        , "city_ru": [] # данный атрибут добавлен для проверки
        , "date": []
        , "hour": []
        , "temperature_c": []
        , "pressure_mm": []
        , "is_rainy":[] # данное значение взято из prec_mm (осадки мм) как показатель того, были ли осадки в принципе, данные не преобразованы в булево. Преобразование будет на стороне sql
    }
    # цикл по генератору с прогнозом по каждому городу
    for frcst in city_json:
        try:
            city_ru=frcst.get("geo_object", None).get("locality").get("name")
            city=city_names.get(city_ru)
            forecast_lst=frcst.get("forecasts")
            # цикл по дню в разрезе города
            for day_ in forecast_lst:
                date=day_.get("date")
                hours=day_.get("hours", None)
                # цикл по часу в разрезе дня
                for hour_ in hours:
                    if hour_: # если hour_ is None, то всем значениям присваиваем значения None
                        hour=hour_.get("hour")
                        temperature_c=hour_.get("temp")
                        pressure_mm=hour_.get("pressure_mm")
                        is_rainy=hour_.get("prec_mm")

                        forecast_dict["city_ru"].append(city_ru)
                        forecast_dict["city"].append(city)
                        forecast_dict["date"].append(date)
                        forecast_dict["hour"].append(hour)
                        forecast_dict["pressure_mm"].append(pressure_mm)
                        forecast_dict["temperature_c"].append(temperature_c)
                        forecast_dict["is_rainy"].append(is_rainy)
                    else: 
                        hour=None
                        temperature_c=None
                        pressure_mm=None
                        is_rainy=None
        except Exception as e:
            print(str(e))
            print(f"error in {city} city")
    return forecast_dict


def create_csv_file(input_data: dict) -> None:
    with open("ya_weather.csv", "w") as ya_weather_csv:
        writer = csv.writer(ya_weather_csv)
        writer.writerow(input_data.keys())
        writer.writerows(zip(*input_data.values()))




if __name__=="__main__":
    ...
    city_json=get_json_weather_for_city()
    result=parse_city_weather_days(city_json=city_json)
    create_csv_file(input_data=result)