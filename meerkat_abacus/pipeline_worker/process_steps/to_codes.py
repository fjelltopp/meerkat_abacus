import logging
from dateutil.parser import parse
from datetime import datetime

from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus import util
from meerkat_abacus.codes import to_codes
from meerkat_abacus.util import data_types
from meerkat_abacus.pipeline_worker.process_steps.write_to_db import get_uuid


class ToCodes(ProcessingStep):

    def __init__(self, param_config, session):
        self.step_name = "to_codes"
        self.config = param_config
        self.links_by_type, self.links_by_name = util.get_links(
            param_config.config_directory +
            param_config.country_config["links_file"])
        self.locations = util.all_location_data(session)
        self.data_types = {d["name"]: d for d in
                           data_types.data_types(param_config=self.config)}
        self.variables = {}
        for type_name, data_type in self.data_types.items():
            self.variables[type_name] = to_codes.get_variables(session,
                                                               match_on_form=data_type["type"])
        self.session = session
    def run(self, form, data):
        return_rows = []
        rows = [data]
        data_type = self.data_types[data["type"]]
        if data_type["multiple_row"]:
            rows = self._get_multi_rows(data, data_type)
        for row in rows:
            # from nose.tools import set_trace; set_trace()
            multiple_forms = set(row.get("link_data", {}).keys())
            row[row["original_form"]] = row["raw_data"]
            for f in multiple_forms:
                row[f] = row["link_data"][f]
            variable_data, category_data, location_data, disregard = to_codes.to_code(
                row, self.variables[data_type["name"]],
                self.locations, data_type["type"],
                self.config.country_config["alert_data"],
                multiple_forms, data_type["location"]
            )
            if location_data is None:
                logging.warning("Missing loc data")
                continue
            row["uuid"] = row[data_type["form"]][data_type["uuid"]]
            epi_year, week, date = self._get_epi_week(row, data_type)
            if epi_year is None:
                continue
            
            variable_data[data_type["var"]] = 1
            variable_data["data_entry"] = 1
            submission_date = None
            if "SubmissionDate" in row[data_type["form"]]:
                submission_date = parse(
                    row[data_type["form"]].get("SubmissionDate")).replace(
                        tzinfo=None)

            links = {}
            for name in data.get("link_data", {}).keys():
                link = self.links_by_name[name]
                links[name] = [x[link["uuid"]] for x in data["link_data"][name]]
            new_data = {
                "date": date,
                "epi_week": week,
                "epi_year": epi_year,
                "submission_date": submission_date,
                "type": data_type["type"],
                "uuid": row[data_type["form"]][data_type["uuid"]],
                "variables": variable_data,
                "categories": category_data,
                "links": links,
                "type_name": data_type["name"]
            }
            new_data.update(location_data)
            return_form = "data"
            if disregard:
                return_form = "disregardedData"
            return_rows.append({"form": return_form,
                                "data": new_data})
        return return_rows

    def _get_multi_rows(self, data, data_type):
        """
        Takes a data row and splits it inro multiple rows based on the 
        config in the data_type
        """
        
        fields = data_type["multiple_row"].split(",")
        i = 1
        data_in_row = True
        sub_rows = []
        while data_in_row:
            data_in_row = False
            sub_row = copy.deepcopy(row)
            for f in fields:
                column_name = f.replace("$", str(i))
                sub_row_name = f.replace("$", "")
                value = row[data_type["form"]].get(column_name, None)
                if value and value != "":
                    sub_row[data_type["form"]][sub_row_name] = value
                    data_in_row = True
            sub_row[data_type["form"]][data_type["uuid"]] = sub_row[data_type["form"]][
                data_type["uuid"]] + ":" + str(i)
            if data_in_row:
                sub_rows.append(sub_row)
            i += 1
        return sub_rows



    def _get_epi_week(self, row, data_type):
        epi_year, week, date = None, None, None
        try:
            date = parse(row[data_type["form"]][data_type["date"]])
            date = datetime(date.year, date.month, date.day)
            epi_year, week = util.epi_week.epi_week_for_date(date,
                                                             param_config=self.config.country_config)
        except KeyError:
            logging.error("Missing Date field %s", data_type["date"])
        except ValueError:
            logging.error(f"Failed to convert date to epi week. uuid: {row.get('uuid', 'UNKNOWN')}")
            logging.debug(f"Faulty row date: {date}.")
        except:
            logging.exception("Invalid Date: %s", row[data_type["form"]].get(data_type["date"]))
        return epi_year, week, date
