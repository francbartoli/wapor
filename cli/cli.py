from click_configfile import ConfigFileReader, Param, SectionSchema
from click_configfile import matches_section
from click import BOOL
import click
import os
import json
import daiquiri
import oauth2client
from ee import EEException, Initialize, ServiceAccountCredentials
from ee.oauth import CLIENT_ID, CLIENT_SECRET
from utils.logging import Log
from utils.helpers import (
    Name, AETIName,
    AGBPName, NBWPName, GBWPName,
    TIME_RESOLUTION as tr
)


class ConfigSectionSchema(object):
    """ Describes all config sections of this configuration file.

        Example:
        # -- FILE: config.cfg
        [wapor]
        gee_workspace_base = projects
        gee_workspace_project = fao_wapor
        [google.fusiontables]
        scope = https://www.googleapis.com/auth/fusiontables
        [google.earthengine]
        scope = https://www.googleapis.com/auth/earthengine

    """

    @matches_section("wapor")
    class Wapor(SectionSchema):
        gee_workspace_base = Param(type=str)
        gee_workspace_project = Param(type=str) 

    @matches_section("google.*")
    class Google(SectionSchema):
        scope = Param(type=str)


class ConfigFileProcessor(ConfigFileReader):
    # @TODO customise filename and searchpath from command option
    config_files = ["config.ini", "config.cfg"]
    config_searchpath = [".", os.path.expanduser("~/.wapor")]
    config_section_schemas = [
        ConfigSectionSchema.Wapor,     # PRIMARY SCHEMA
        ConfigSectionSchema.Google,
    ]


class CredentialConfigFile(dict):
    def __init__(self, credential_file):
            self.credential_file = credential_file

    def load(self):
        """load a JSON Service Account file from disk"""
        with open(self.credential_file) as cf:
            try:
                return json.loads(
                    cf.read()
                )
            except Exception as e:
                raise click.Abort()

    def get_sa_credentials(self, scopes):
        """Authenticate with a Service account.
        """
        try:
            account = self.load()["client_email"]
            return ServiceAccountCredentials(
                account,
                self.credential_file,
                scopes
            )
        except EEException as eee:
            logger.debug(
                "Error with GEE authentication ======> {0}".format(
                    eee.message
                )
            )
            raise click.Abort()
        except KeyError as e:
            logger.debug("Service account file doesn't have the key 'client_email'!")
            raise click.Abort()


class CredentialFile(click.ParamType):
    name = 'service-account'

    def convert(self, value, param, ctx):
        if value:
            return value


class Level(click.ParamType):
    name = 'level'

    def convert(self, value, param, ctx):
        return value


class ApiKey(click.ParamType):
    name = 'api-key'

    def convert(self, value, param, ctx):
        if value:
            try:
                creds = oauth2client.client.OAuth2Credentials(
                    None, CLIENT_ID, CLIENT_SECRET,
                    value, None,
                    'https://accounts.google.com/o/oauth2/token', None
                )
                return creds
            except EEException as eee:
                logger.debug(
                    "Error with GEE authentication ======> {0}".format(
                        eee.message
                    )
                )
                raise click.Abort()


class Logging(click.ParamType):
    name = 'verbose'

    def convert(self, value, param, ctx):
        # @TODO convert to possible logging values
        return value


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
    help='your API authentication token for Earth Engine API',
)
@click.option(
    '--service-account', '-s',
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
@click.option(
    '--export', '-e',
    type=BOOL,
    default=True,
    help='Return intermediate outputs for inputs components (default=False)',
)
@click.option(
    '--outputs', '-o',
    type=BOOL,
    default=False,
    help='Return intermediate outputs for inputs components (default=False)',
)
@click.pass_context
def main(
    ctx, verbose, api_key, credential_file,
    config_file, level, export, outputs
):
    """
    """

    Log(verbose).initialize()
    logger = daiquiri.getLogger(ctx.command.name, subsystem="MAIN")
    logger.info(
        "================ {0} =================".format(
            ctx.command.name
        )
    )
    # --config-file
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

    scopes = []
    for wapor_data_key in ctx.default_map.keys():
        if wapor_data_key.startswith("google."):
            scopes.append(ctx.default_map[wapor_data_key]["scope"])
        if wapor_data_key == "gee_workspace_base":
            ee_workspace_base = ctx.default_map[wapor_data_key]
        if wapor_data_key == "gee_workspace_project":
            ee_workspace_wapor = ctx.default_map[wapor_data_key]
    logger.debug("Scopes =====> {0}".format(scopes))
    # --credential-file
    credentials = os.path.expanduser(credential_file)
    logger.debug(
        "Credential file =====> {0}".format(credentials)
    )
    if os.path.exists(credentials):
        logger.info(
            "Authenticate with Service Account {0}".format(credentials)
        )
        auth = Initialize(
            CredentialConfigFile(credentials).get_sa_credentials(scopes)
        )
    elif api_key:
        logger.info(
            "Authenticate with Api Key {0}".format(api_key)
        )
        auth = Initialize(api_key)
    else:
        logger.info(
            "Neither Api Key nor Service Account has been provided!\
Please check the default Service Account file {0}".format(
                credentials
            )
        )
        raise click.Abort()

    ctx.obj = {
        'auth': auth,
        'EE_WORKSPACE_BASE': ee_workspace_base,
        'EE_WORKSPACE_WAPOR': os.path.join(
            ee_workspace_base,
            ee_workspace_wapor
        ),
        'level': level,
        'export': export,
        'outputs': outputs
    }


@main.command() 
@click.argument('year', type=click.Choice(
    [
        "2009",
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018"
    ]
))
@click.argument('temporal_resolution', type=click.Choice(["A", "D"]))
@click.argument('input_component', type=click.Choice(
    ["E", "T", "I", "AETI", "NPP"]
))
@click.argument('nodatavalue', type=click.Choice(
    ["255", "-9999"]
), required=0)
@click.pass_context 
def common(ctx, year, temporal_resolution, input_component, nodatavalue): 
    """
        YEAR 2009|2010|...|2017|2018\n
        TEMPORAL_RESOLUTION A|D\n
        INPUT_COMPONENT E|T|I|AETI|NPP\n
        NODATAVALUE: 255|-9999\n

        example annual: wapor -l L1 common -- 2016 A E (255)
        example dekadal: wapor -l L1 common -- 2016 D E
    """

    Log("DEBUG").initialize()
    logger = daiquiri.getLogger(ctx.command.name, subsystem="COMMON")
    logger.info(
        "================ {0} {1} calculation =================".format(
            ctx.command.name,
            temporal_resolution
        )
    )

    from algorithms.common import Common

    kwargs = { 
        "year": year,  
        "temporal_resolution": temporal_resolution, 
        "component": input_component,
        "nodatavalue": nodatavalue
    } 
    context = ctx.obj.copy() 
    context.update(kwargs) 

    # Use class Name to express wapor name convention over GEE
    src_image_coll = Name(**context).src_collection()
    # L1_E_D, L1_T_D, L1_I_D
    logger.debug(
        "src_image_coll variable =====> {0}".format(src_image_coll)
    )
    dst_image_coll = Name(**context).dst_collection()
    # L1_E_A, L1_T_A, L1_I_A
    logger.debug(
        "dst_image_coll variable =====> {0}".format(dst_image_coll)
    )
    dst_asset_coll = Name(**context).dst_assetcollection_id()
    # projects/fao_wapor/L1_E_A
    logger.debug(
        "dst_asset_coll variable =====> {0}".format(dst_asset_coll)
    )
    dst_asset_image = Name(**context).dst_image()
    # L1_E_16
    logger.debug(
        "dst_asset_image variable =====> {0}".format(dst_asset_image)
    )
    dst_asset_id = Name(**context).dst_asset_id()
    # projects/fao_wapor/L1_E_A/L1_E_16
    logger.debug(
        "dst_asset_id variable =====> {0}".format(dst_asset_id)
    ) 

    kwargs.update(
        {
            "src_coll": os.path.join(
                os.path.join(
                    context["EE_WORKSPACE_WAPOR"],
                    context["level"]
                ),
                src_image_coll
            ),
            "dst_coll": dst_image_coll,
            "dst_asset_coll": dst_asset_coll,
            "dst_asset": dst_asset_id,
            "to_asset": context["export"],
            "intermediate_outputs": context["outputs"]
        }
    )
    logger.debug(
        "Input kwargs dictionary for Common process is =====> \n{0}".format(
            json.dumps(kwargs)
        )
    )

    # create the instance of the common script class
    proc = Common(**kwargs)
    # run the process and return the task id
    result = proc.process_annual()

    if result["errors"]:
        raise click.ClickException(
            "Commad execution has produced:\n{0}".format(
                json.dumps(result)
            )
        )
    else:
        click.echo(
            json.dumps(result)
        )

@main.command()
@click.argument('year', type=click.Choice(
    [
        "2009",
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018"
    ]
))
@click.argument('temporal_resolution', type=click.Choice(["D"]))
@click.argument('input_component', type=click.Choice(["AETI"]))
@click.argument('dekad', type=click.Choice(
    [
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
        "31", "32", "33", "34", "35", "36"
    ]
), required=0)
@click.pass_context
def aeti(ctx, year, temporal_resolution, input_component, dekad):
    """
        YEAR 2009|2010|...|2017|2018\n
        TEMPORAL_RESOLUTION A (ANNUAL)|D (DEKADAL)\n
        INPUT_COMPONENT AETI\n
        DEKAD: 01|02|...|36\n

        example: wapor -l L1 aeti -- 2016 D AETI
        example: wapor -l L1 aeti -- 2016 D AETI 01
    """

    Log("DEBUG").initialize()
    logger = daiquiri.getLogger(ctx.command.name, subsystem="AETI")
    logger.info(
        "================ {0} {1} calculation =================".format(
            ctx.command.name,
            temporal_resolution
        )
    )

    from algorithms.aeti import AETI

    kwargs = {
        "year": year,
        "temporal_resolution": temporal_resolution,
        "component": input_component, #  AETI
        "dekad": dekad
    }
    context = ctx.obj.copy()
    context.update(kwargs)

    # Use class AETIName to express wapor name convention over GEE
    src_image_coll = AETIName(**context).src_collection()
    # L1_E_D, L1_T_D, L1_I_D
    logger.debug(
        "src_image_coll variable =====> {0}".format(src_image_coll)
    )
    dst_image_coll = AETIName(**context).dst_collection()
    # L1_AETI_D
    logger.debug(
        "dst_image_coll variable =====> {0}".format(dst_image_coll)
    )
    dst_asset_coll = AETIName(**context).dst_assetcollection_id()
    # projects/fao_wapor/L1_AETI_D
    logger.debug(
        "dst_asset_coll variable =====> {0}".format(dst_asset_coll)
    )
    dst_asset_images = AETIName(**context).dst_images()
    # [AETI_1601,...,AETI_1636]
    logger.debug(
        "dst_asset_images variable =====> {0}".format(dst_asset_images)
    )
    dst_asset_ids = AETIName(**context).dst_asset_ids()
    # [projects/fao_wapor/L1_AETI_D/L1_AETI_1601,...,
    # ,...,projects/fao_wapor/L1_AETI_D/L1_AETI_1636]
    logger.debug(
        "dst_asset_ids variable =====> {0}".format(dst_asset_ids)
    )
    if "AETI" in dst_image_coll:
        if context["temporal_resolution"] in [
            tr.dekadal.value,
            tr.short_dekadal.value
        ]:
            # projects/fao_wapor/L1_E_D
            e = os.path.join(
                os.path.join(
                    context["EE_WORKSPACE_WAPOR"],
                    context["level"]
                ),
                src_image_coll[0]
            )
            # projects/fao_wapor/L1_T_D
            t = os.path.join(
                os.path.join(
                    context["EE_WORKSPACE_WAPOR"],
                    context["level"]
                ),
                src_image_coll[1]
            )
            # projects/fao_wapor/L1_I_D
            i = os.path.join(
                os.path.join(
                    context["EE_WORKSPACE_WAPOR"],
                    context["level"]
                ),
                src_image_coll[2]
            )

            colls = {"collI": i, "collE": e, "collT": t}
            kwargs.update(colls)

            kwargs.update(
                {
                    "dst_coll": dst_image_coll,
                    "dst_asset_coll": dst_asset_coll,
                    "dst_assets": dst_asset_ids,
                    "to_asset": context["export"],
                    "intermediate_outputs": context["outputs"]
                }
            )
            logger.debug(
                "Input kwargs dictionary for AETI process is =====> \n{0}".format(
                    json.dumps(kwargs)
                )
            )

            # Create Marmee object instance with specific inputs for AETI and filter
            aeti = AETI(**kwargs)

            # Run the process for dekadal
            try:
                # run the process and return the task id
                result = aeti.process_dekadal()

                if result["errors"]:
                    raise click.ClickException(
                        "Commad execution has produced:\n{0}".format(
                            json.dumps(result)
                        )
                    )
                else:
                    click.echo(
                        json.dumps(result)
                    )
            except Exception as e:
                raise

        elif context["temporal_resolution"] in [
            tr.annual.value,
            tr.short_annual.value
        ]:
            pass
            # Run the process for annual
            try:
                pass
                aeti.process_annual()
            except Exception as e:
                raise
        else:
            raise ValueError("Not allowed for wapor annual or dekadal.")
    else:
        raise ValueError("Wrong value for algorithm not being AETI")


@main.command()
@click.argument('year', type=click.Choice(
    [
        "2009",
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018"
    ]
))
@click.argument('temporal_resolution', type=click.Choice(["A"]))
@click.argument('input_component', type=click.Choice(["NPP"]), required=0)
@click.argument('nodatavalue', type=click.Choice(
    ["-9999"]
), required=0)
@click.pass_context
def AGBP(ctx, year, temporal_resolution, input_component, nodatavalue):
    """
        YEAR 2009|2010|...|2017|2018\n
        TEMPORAL_RESOLUTION A (ANNUAL)\n
        INPUT_COMPONENT NPP\n
        NODATAVALUE -9999\n

        example annual: wapor -l L1 agbp -- 2016 A NPP (-9999)
    """

    Log("DEBUG").initialize()
    logger = daiquiri.getLogger(ctx.command.name, subsystem="AGBP")
    logger.info(
        "================ {0} {1} calculation =================".format(
            ctx.command.name,
            temporal_resolution
        )
    )

    from algorithms.agbp import AGBP

    kwargs = {
        "year": year,
        "temporal_resolution": temporal_resolution,
        "component": input_component,
        "nodatavalue": nodatavalue
    }
    context = ctx.obj.copy()
    context.update(kwargs)

    # Use class Name to express wapor name convention over GEE
    src_image_coll = AGBPName(**context).src_collection()
    # L1_NPP_D
    logger.debug(
        "AGBP src_image_coll variable =====> {0}".format(src_image_coll)
    )
    dst_image_coll = AGBPName(**context).dst_collection()
    # L1_AGBP_A
    logger.debug(
        "AGBP dst_image_coll variable =====> {0}".format(dst_image_coll)
    )
    dst_asset_coll = AGBPName(**context).dst_assetcollection_id()
    # projects/fao_wapor/L1_AGBP_A
    logger.debug(
        "AGBP dst_asset_coll variable =====> {0}".format(dst_asset_coll)
    )
    dst_asset_image = AGBPName(**context).dst_image()
    # L1_AGBP_16
    logger.debug(
        "AGBP dst_asset_image variable =====> {0}".format(dst_asset_image)
    )
    dst_asset_id = AGBPName(**context).dst_asset_id()
    # projects/fao_wapor/L1_AGBP_A/L1_AGBP_16
    logger.debug(
        "AGBP dst_asset_id variable =====> {0}".format(dst_asset_id)
    )

    kwargs.update(
        {
            "src_coll": os.path.join(
                os.path.join(
                    context["EE_WORKSPACE_WAPOR"],
                    context["level"]
                ),
                src_image_coll
            ),
            "dst_coll": dst_image_coll,
            "dst_asset_coll": dst_asset_coll,
            "dst_asset": dst_asset_id,
            "to_asset": context["export"],
            "intermediate_outputs": context["outputs"]
        }
    )
    logger.debug(
        "Input kwargs dictionary for AGBP process is =====> \n{0}".format(
            json.dumps(kwargs)
        )
    )

    # create the instance of the agbp script class
    proc = AGBP(**kwargs)
    # run the process and return the task id
    result = proc.process_annual()

    if result["errors"]:
        raise click.ClickException(
            "Commad execution has produced:\n{0}".format(
                json.dumps(result)
            )
        )
    else:
        click.echo(
            json.dumps(result)
        )


@main.command()
@click.argument('year', type=click.Choice(
    [
        "2009",
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018"
    ]
))
@click.argument('temporal_resolution', type=click.Choice(["A"]))
@click.argument('input_component', type=click.Choice(["AGBP"]), required=0)
@click.argument('nodatavalue', type=click.Choice(
    ["-9999"]
), required=0)
@click.pass_context
def NBWP(ctx, year, temporal_resolution, input_component, nodatavalue):
    """
        YEAR 2009|2010|...|2017|2018\n
        TEMPORAL_RESOLUTION A (ANNUAL)\n
        INPUT_COMPONENT AGBP\n
        NODATAVALUE -9999\n

        example annual: wapor -l L1 nbwp -- 2016 A AGBP (-9999)
    """

    Log("DEBUG").initialize()
    logger = daiquiri.getLogger(ctx.command.name, subsystem="NBWP")
    logger.info(
        "================ {0} {1} calculation =================".format(
            ctx.command.name,
            temporal_resolution
        )
    )

    from algorithms.nbwp import NBWP

    kwargs = {
        "year": year,
        "temporal_resolution": temporal_resolution,
        "component": input_component,
        "nodatavalue": nodatavalue
    }
    context = ctx.obj.copy()
    context.update(kwargs)

    # Use class Name to express wapor name convention over GEE
    src_image_coll = NBWPName(**context).src_collection()
    # L1_AGBP_A
    logger.debug(
        "NBWP src_image_coll variable =====> {0}".format(src_image_coll)
    )
    dst_image_coll = NBWPName(**context).dst_collection()
    # L1_NBWP_A
    logger.debug(
        "NBWP dst_image_coll variable =====> {0}".format(dst_image_coll)
    )
    dst_asset_coll = NBWPName(**context).dst_assetcollection_id()
    # projects/fao_wapor/L1_NBWP_A
    logger.debug(
        "NBWP dst_asset_coll variable =====> {0}".format(dst_asset_coll)
    )
    dst_asset_image = NBWPName(**context).dst_image()
    # L1_NBWP_16
    logger.debug(
        "NBWP dst_asset_image variable =====> {0}".format(dst_asset_image)
    )
    dst_asset_id = NBWPName(**context).dst_asset_id()
    # projects/fao_wapor/L1_NBWP_A/L1_NBWP_16
    logger.debug(
        "NBWP dst_asset_id variable =====> {0}".format(dst_asset_id)
    )

    kwargs.update(
        {
            "src_coll": os.path.join(
                os.path.join(
                    context["EE_WORKSPACE_WAPOR"],
                    context["level"]
                ),
                src_image_coll
            ),
            "dst_coll": dst_image_coll,
            "dst_asset_coll": dst_asset_coll,
            "dst_asset": dst_asset_id,
            "to_asset": context["export"],
            "intermediate_outputs": context["outputs"]
        }
    )
    logger.debug(
        "Input kwargs dictionary for NBWP process is =====> \n{0}".format(
            json.dumps(kwargs)
        )
    )

    # create the instance of the nbwp script class
    proc = NBWP(**kwargs)
    # run the process and return the task id
    result = proc.process_annual()

    if result["errors"]:
        raise click.ClickException(
            "Commad execution has produced:\n{0}".format(
                json.dumps(result)
            )
        )
    else:
        click.echo(
            json.dumps(result)
        )


@main.command()
@click.argument('year', type=click.Choice(
    [
        "2009",
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018"
    ]
))
@click.argument('temporal_resolution', type=click.Choice(["A"]))
@click.argument('input_component', type=click.Choice(["AGBP"]), required=0)
@click.argument('nodatavalue', type=click.Choice(
    ["-9999"]
), required=0)
@click.pass_context
def GBWP(ctx, year, temporal_resolution, input_component, nodatavalue):
    """
        YEAR 2009|2010|...|2017|2018\n
        TEMPORAL_RESOLUTION A (ANNUAL)\n
        INPUT_COMPONENT AGBP\n
        NODATAVALUE -9999\n

        example annual: wapor -l L1 gbwp -- 2016 A AGBP (-9999)
    """

    Log("DEBUG").initialize()
    logger = daiquiri.getLogger(ctx.command.name, subsystem="GBWP")
    logger.info(
        "================ {0} {1} calculation =================".format(
            ctx.command.name,
            temporal_resolution
        )
    )
    from algorithms.gbwp import GBWP

    kwargs = {
        "year": year,
        "temporal_resolution": temporal_resolution,
        "component": input_component,
        "nodatavalue": nodatavalue
    }
    context = ctx.obj.copy()
    context.update(kwargs)

    # Use class Name to express wapor name convention over GEE
    src_image_coll = GBWPName(**context).src_collection()
    # L1_AGBP_A
    logger.debug(
        "GBWP src_image_coll variable =====> {0}".format(src_image_coll)
    )
    dst_image_coll = GBWPName(**context).dst_collection()
    # L1_GBWP_A
    logger.debug(
        "GBWP dst_image_coll variable =====> {0}".format(dst_image_coll)
    )
    dst_asset_coll = GBWPName(**context).dst_assetcollection_id()
    # projects/fao_wapor/L1_GBWP_A
    logger.debug(
        "GBWP dst_asset_coll variable =====> {0}".format(dst_asset_coll)
    )
    dst_asset_image = GBWPName(**context).dst_image()
    # L1_GBWP_16
    logger.debug(
        "GBWP dst_asset_image variable =====> {0}".format(dst_asset_image)
    )
    dst_asset_id = GBWPName(**context).dst_asset_id()
    # projects/fao_wapor/L1_GBWP_A/L1_GBWP_16
    logger.debug(
        "GBWP dst_asset_id variable =====> {0}".format(dst_asset_id)
    )

    kwargs.update(
        {
            "src_coll": os.path.join(
                os.path.join(
                    context["EE_WORKSPACE_WAPOR"],
                    context["level"]
                ),
                src_image_coll
            ),
            "dst_coll": dst_image_coll,
            "dst_asset_coll": dst_asset_coll,
            "dst_asset": dst_asset_id,
            "to_asset": context["export"],
            "intermediate_outputs": context["outputs"]
        }
    )
    logger.debug(
        "Input kwargs dictionary for GBWP process is =====> \n{0}".format(
            json.dumps(kwargs)
        )
    )

    # create the instance of the gbwp script class
    proc = GBWP(**kwargs)
    # run the process and return the task id
    result = proc.process_annual()

    if result["errors"]:
        raise click.ClickException(
            "Commad execution has produced:\n{0}".format(
                json.dumps(result)
            )
        )
    else:
        click.echo(
            json.dumps(result)
        )

if __name__ == "__main__":
    main()
