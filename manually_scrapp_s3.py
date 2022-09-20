from includes import get_matches_ids_4_weeks, scrapp_xg_xa_uderstat

list_of_matches = get_matches_ids_4_weeks()

for l in list_of_matches:
    scrapp_xg_xa_uderstat(l)