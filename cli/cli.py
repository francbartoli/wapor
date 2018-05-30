from click_configfile import ConfigFileReader, Param, SectionSchema
from click_configfile import matches_section
import click
import os
import json
import daiquiri
import oauth2client
from ee import EEException, Initialize, ServiceAccountCredentials
from ee.oauth import CLIENT_ID, CLIENT_SECRET
from utils.logging import Log
from utils.helpers import Name, TIME_RESOLUTION as tr


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
    name = 'credential-file'

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
@click.argument('year') 
@click.argument('temporal_resolution') 
@click.argument('input_component') 
@click.pass_context 
def common(ctx, year, temporal_resolution, input_component): 
    """
        example: wapor -l L1 common 2016 D E
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
        "component": input_component 
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
    click.echo(
        json.dumps(result)
    )

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
