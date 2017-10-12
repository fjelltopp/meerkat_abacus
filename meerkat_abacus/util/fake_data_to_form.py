import pyxform

from meerkat_abacus.config import config

country_config = config.country_config

from pyxform import builder, xls2json
from pyxform.utils import has_external_choices, sheet_to_csv





for form in country_config["fake_data"].keys():
    json_survey = {
        'type': 'survey', 'name': form, 'title': form, 'id_string': form, 'sms_keyword': 'sample', 'default_language': 'default', 'children': []
        }
    
    groups = {}
    for field in ["start", "end", "today", "deviceid"]:
        json_survey["children"].append(
            {'hint': 'hint',
             'type': "text",
             'name': field,
             'label': 'Label'})
    for field, type_info in country_config["fake_data"][form].items():
        ty = "text"
        if list(type_info.keys())[0] == "integer":
            ty = "integer"

        if "./" in field:
            # Create a group
            group_name, field_name = field.split("./")

            if not group_name in groups.keys():
                json_survey["children"].append(
                    {'control': {'appearance': 'field-list'},
                     'type': 'group',
                     'name': group_name,
                     'label': 'A group',
                     'children': [
                         {'hint': 'hint',
                          'type': ty,
                          'name': field_name,
                          'label': 'Label'}
                     ]
                    }
                )
                groups[group_name] = json_survey["children"][-1]["children"]
            else:
                groups[group_name].append(
                     {'hint': 'hint',
                      'type': ty,
                      'name': field_name,
                      'label': 'Label'}
                    )
        else:
            json_survey["children"].append(
                {'hint': 'hint',
                 'type': ty,
                 'name': field,
                 'label': 'Label'})
    
    survey = builder.create_survey_element_from_dict(json_survey)
    # Setting validate to false will cause the form not to be processed by
    # ODK Validate.
    # This may be desirable since ODK Validate requires launching a subprocess
    # that runs some java code.
    survey.print_xform_to_file(
        form +".xml", validate=False, warnings=True)
