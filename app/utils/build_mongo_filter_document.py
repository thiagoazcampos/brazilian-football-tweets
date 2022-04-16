from config.teams import teams_pretty_mapping

def build_mongo_filter_document(start_date, end_date, team, date_str_format="%Y-%m-%d"):
    mongo_filter_document = {
        "created_at": {
            "$gte": start_date.strftime(date_str_format),
            "$lte": end_date.strftime(date_str_format)
        }
    }
    if team != 'All':
        mongo_filter_document.update({'team': teams_pretty_mapping[team]})
    
    return mongo_filter_document