from click_configfile import ConfigFileReader, Param, SectionSchema
from click_configfile import matches_section
import click
import os
import json
import daiquiri
from utils.logging import Log
from utils.helpers import Name, TIME_RESOLUTION as tr


class ConfigSectionSchema(object):
    """ Describes all config sections of this configuration file.

        Example:
        # -- FILE: config.cfg
        [wapor]
        gee_workspace_base = projects
        gee_workspace_project = fao_wapor
        
    """

    @matches_section("wapor")
    class Wapor(SectionSchema):
        gee_workspace_base = Param(type=str)
        gee_workspace_project = Param(type=str) 
        

class ConfigFileProcessor(ConfigFileReader):
    # @TODO customise filename and searchpath from command option
    config_files = ["config.ini", "config.cfg"]
    config_searchpath = [".", os.path.expanduser("~/.wapor")]
    config_section_schemas = [
        ConfigSectionSchema.Wapor,     # PRIMARY SCHEMA
    ]


class Level(click.ParamType):
    name = 'level'

    def level(args):
        pass


class ApiKey(click.ParamType):
    name = 'api-key'

    def apikey(args):
        pass


class Logging(click.ParamType):
    name = 'verbose'

    def logging(args):
        pass


CONTEXT_SETTINGS = dict(default_map=ConfigFileProcessor.read_config())

@click.group()
@click.option(
    '--verbose', '-v',
    type=Logging(),
    help='Verbosity of logging output',
    default='INFO',
)
@click.option(
    '--api-key', '-a',
    type=ApiKey(),
    help='your API key for Earth Engine API',
)
@click.option(
    '--credential-file', '-c',
    type=click.Path(),
    default='~/.wapor/serviceaccount.json',
)
@click.option(
    '--config-file', '-c',
    type=click.Path(),
    default='~/.wapor/config.cfg',
)
@click.option(
    '--level', '-l',
    type=Level(),
    help='Level of the data - Can be L1, L2, L3',
)
@click.pass_context
def main(ctx, verbose, api_key, credential_file, config_file, level):
    """
    """

    Log(verbose).initialize()
    logger = daiquiri.getLogger(ctx.command.name, subsystem="MAIN")
    logger.info(
        "================ {0} =================".format(
            ctx.command.name
        )
    )

    fn_credential = os.path.expanduser(credential_file)
    logger.debug(
        "Credential file =====> {0}".format(fn_credential)
    )
    if not api_key and os.path.exists(fn_credential):
        with open(fn_credential) as crd:
            api_key = crd.read()

    fn_config = os.path.expanduser(config_file)
    logger.debug(
        "Configuration file =====> {0}".format(fn_config)
    )
    if os.path.exists(fn_config):
        ctx.default_map = CONTEXT_SETTINGS['default_map']
        logger.debug(
            "Context with default map =====> {0}".format(
                json.dumps(ctx.default_map)
            )
        )
    else:
        logger.critical(
            "Configuration file {0} is not available!".format(
                fn_config
            )
        )
        raise click.Abort()

    try:
        pass
    except KeyError as e:
        logger.debug("Error =====> {0}".format(
            e.message
        ))
        raise
    for wapor_data_key in ctx.default_map.keys():
        if wapor_data_key == "gee_workspace_base":
            ee_workspace_base = ctx.default_map[wapor_data_key]
        if wapor_data_key == "gee_workspace_project":
            ee_workspace_wapor = ctx.default_map[wapor_data_key]

    ctx.obj = {
        'api_key': api_key,
        'credential_file': fn_credential,
        'EE_WORKSPACE_BASE': ee_workspace_base,
        'EE_WORKSPACE_WAPOR': os.path.join(
            ee_workspace_base,
            ee_workspace_wapor
        ),
        'level': level
    }


@main.command()
@click.argument('year')
@click.argument('season')
@click.argument('temporal_resolution')
@click.argument('input_component')
@click.pass_context
def aet(ctx, year, season, temporal_resolution, input_component):
    """
        wapor -l L1 aet 2016 1 A AET
    """

    from algorithms.aet import AET 

    kwargs = {
        "year": year,
        "season": season,
        "temporal_resolution": temporal_resolution,
        "component": input_component #  AET
    }
    context = ctx.obj.copy()
    context.update(kwargs)

    # Use class Name to express wapor name convention over GEE
    src_image_coll = Name(**context).src_collection()
    print src_image_coll  # L1_AET
    dst_image_coll = Name(**context).dst_collection()
    print dst_image_coll  # L1_AET_A
    dst_asset_coll = Name(**context).dst_assetcollection_id()
    print dst_asset_coll  # projects/fao_wapor/L1_AET_A
    dst_asset_image = Name(**context).dst_image()
    print dst_asset_image  # AET_16
    dst_asset_id = Name(**context).dst_asset_id()
    print dst_asset_id  # projects/fao_wapor/L1_AET_A/AET_16
    if "AET" in src_image_coll:
        if context["temporal_resolution"] in [
            tr.annual.value,
            tr.short_annual.value
        ]:
            i = os.path.join(
                context["EE_WORKSPACE_WAPOR"],
                src_image_coll.replace("AET", "I")
            ) + "_" + tr.short_dekadal.value
@main.command()
@click.argument('year')
@click.argument('temporal_resolution')
@click.argument('input_component')
@click.pass_context
def eti(ctx, year, temporal_resolution, input_component):
    """
        wapor -l L1 eti 2016 D ETI
    """

    Log("DEBUG").initialize()
    logger = daiquiri.getLogger(ctx.command.name, subsystem="ETI")
    logger.info(
        "================ {0} {1} calculation =================".format(
            ctx.command.name,
            temporal_resolution
        )
    )

    from algorithms.eti import ETI

    kwargs = {
        "year": year,
        "temporal_resolution": temporal_resolution,
        "component": input_component #  ETI
    }
    context = ctx.obj.copy()
    context.update(kwargs)

    # Use class Name to express wapor name convention over GEE
    src_image_coll = Name(**context).src_collection()
    # L1_E, L1_T, L1_I
    logger.debug(
        "src_image_coll variable =====> {0}".format(src_image_coll)
    )
    dst_image_coll = Name(**context).dst_collection()
    # L1_AET_A
    logger.debug(
        "dst_image_coll variable =====> {0}".format(dst_image_coll)
    )
    dst_asset_coll = Name(**context).dst_assetcollection_id()
    # projects/fao_wapor/L1_AET_A
    logger.debug(
        "dst_asset_coll variable =====> {0}".format(dst_asset_coll)
    )
    dst_asset_image = Name(**context).dst_image()
    # AET_16
    logger.debug(
        "dst_asset_image variable =====> {0}".format(dst_asset_image)
    )
    dst_asset_id = Name(**context).dst_asset_id()
    # projects/fao_wapor/L1_AET_A/L1_AET_16
    logger.debug(
        "dst_asset_id variable =====> {0}".format(dst_asset_id)
    )
    if "ETI" in src_image_coll:
        if context["temporal_resolution"] in [
            tr.annual.value,
            tr.short_annual.value
        ]:
            # projects/fao_wapor/L1_E_D
            e = os.path.join(
                        context["EE_WORKSPACE_WAPOR"], 
                        src_image_coll.replace("ETI", "E")
                    ) + "_" + tr.short_dekadal.value
            # projects/fao_wapor/L1_T_D
            t = os.path.join(
                        context["EE_WORKSPACE_WAPOR"], 
                        src_image_coll.replace("ETI", "T")
                    ) + "_" + tr.short_dekadal.value
            # projects/fao_wapor/L1_I_D
            i = os.path.join(
                    context["EE_WORKSPACE_WAPOR"],
                    src_image_coll.replace("ETI", "I")
                ) + "_" + tr.short_dekadal.value
        else:
            raise ValueError("Not implemented yet.")
    else:
        raise ValueError("Wrong value for algorithm not being ETI")
    colls = {"collI": i, "collE": e, "collT": t}
    
    # Create Marmee object instance with specific inputs for ETI and filter
    eti = ETI(**colls)
    self.logger.debug(
        "Received inputs in STAC format are =====>\n{0}".format(
            self.inputs.json
        )
    )
    print eti.inputs.json

    # Run the process for annual
    for context["temporal_resolution"] in [
        tr.annual.value,
        tr.short_annual.value
    ]:
        try:
            pass
            aet.process_annual()
        except Exception as e:
            raise


if __name__ == "__main__":
    main()
