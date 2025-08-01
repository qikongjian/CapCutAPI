from pyJianYingDraft import trange, Video_scene_effect_type, Video_character_effect_type, CapCut_Video_scene_effect_type, CapCut_Video_character_effect_type, exceptions
import pyJianYingDraft as draft
from typing import Optional, Dict, List, Union
from create_draft import get_or_create_draft
from tools.redis_cache import update_cache
from util import generate_draft_url
from settings import IS_CAPCUT_ENV

def add_effect_impl(
    effect_type: str,  # Changed to string type
    start: float = 0,
    end: float = 3.0,
    draft_id: Optional[str] = None,
    track_name: Optional[str] = "effect_01",
    params: Optional[List[Optional[float]]] = None,
    width: int = 1080,
    height: int = 1920
) -> Dict[str, str]:
    """
    Add an effect to the specified draft
    :param effect_type: Effect type name, will be matched from Video_scene_effect_type or Video_character_effect_type
    :param start: Start time (seconds), default 0
    :param end: End time (seconds), default 3 seconds
    :param draft_id: Draft ID, if None or corresponding zip file not found, a new draft will be created
    :param track_name: Track name, can be omitted when there is only one effect track
    :param params: Effect parameter list, items not provided or None in the parameter list will use default values
    :param width: Video width, default 1080
    :param height: Video height, default 1920
    :return: Updated draft information
    """
    # Get or create draft
    draft_id, script = get_or_create_draft(
        draft_id=draft_id,
        width=width,
        height=height
    )

    # Calculate time range
    duration = end - start
    t_range = trange(f"{start}s", f"{duration}s")

    # Dynamically get effect type object
    if IS_CAPCUT_ENV:
        # If in CapCut environment, use CapCut effects
        effect_enum = CapCut_Video_scene_effect_type[effect_type]
        if effect_enum is None:
            effect_enum = CapCut_Video_character_effect_type[effect_type]
    else:
        # Default to using JianYing effects
        effect_enum = Video_scene_effect_type[effect_type]
        if effect_enum is None:
            effect_enum = Video_character_effect_type[effect_type]
    
    if effect_enum is None:
        raise ValueError(f"Unknown effect type: {effect_type}")

    # Add effect track (only when track doesn't exist)
    if track_name is not None:
        try:
            imported_track=script.get_imported_track(draft.Track_type.effect, name=track_name)
            # If no exception is thrown, the track already exists
        except exceptions.TrackNotFound:
            # Track doesn't exist, create a new track
            script.add_track(draft.Track_type.effect, track_name=track_name)
    else:
        script.add_track(draft.Track_type.effect)

    # Add effect
    script.add_effect(effect_enum, t_range, params=params[::-1], track_name=track_name)

    # 重要：将修改后的 script 重新保存到缓存中
    update_cache(draft_id, script)

    return {
        "draft_id": draft_id,
        "draft_url": generate_draft_url(draft_id)
    }
