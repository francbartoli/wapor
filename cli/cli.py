import click
import os
from utils.helpers import Name


class Level(click.ParamType):
    name = 'level'

    def level(args):
        pass


class ApiKey(click.ParamType):
    name = 'api-key'

    def apikey(args):
        pass


@click.group()
@click.option(
    '--api-key', '-a',
    type=ApiKey(),
    help='your API key for Earth Engine API',
)
@click.option(
    '--credential-file', '-c',
    type=click.Path(),
    default='~/.serviceaccount.cfg',
)
@click.option(
    '--config-file', '-c',
    type=click.Path(),
    default='config.yaml',
)
@click.option(
    '--level', '-l',
    type=Level(),
    help='Level of the data - Can be L1, L2, L3',
)
@click.pass_context
def main(ctx, api_key, credential_file, config_file, level):
    """
    """
    fn_credential = os.path.expanduser(credential_file)
    if not api_key and os.path.exists(fn_credential):
        with open(fn_credential) as cfg:
            api_key = cfg.read()
    
    fn_config = os.path.expanduser(config_file)
    # TODO

    ee_workspace_base = "projects"
    ee_workspace_wapor = "fao_wapor"

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
    kwargs = {
        "year": year,
        "season": season,
        "temporal_resolution": temporal_resolution,
        "component": input_component
    }
    context = ctx.obj.copy()
    context.update(kwargs)

    src_image_coll = Name(**context).src_collection()
    print src_image_coll
    dst_image_coll = Name(**context).dst_collection()
    print dst_image_coll
    dst_asset_coll = Name(**context).dst_assetcollection_id()
    print dst_asset_coll
    dst_asset_image = Name(**context).dst_image()
    print dst_asset_image
    dst_asset_id = Name(**context).dst_asset_id()
    print dst_asset_id

if __name__ == "__main__":
    main()
