import uuid
import pyJianYingDraft as draft
import time
from tools.redis_cache import DRAFT_CACHE, update_cache

def create_draft(draft_id, width=1920, height=1080):
    """
    Create new CapCut draft with specified draft_id
    :param draft_id: Draft ID (required)
    :param width: Video width, default 1920
    :param height: Video height, default 1080
    :return: (script, draft_id)
    """
    # Create CapCut draft with specified resolution
    script = draft.Script_file(width, height)
    
    # Store in global cache
    update_cache(draft_id, script)
    
    return script, draft_id

def get_or_create_draft(draft_id=None, width=1920, height=1080):
    """
    Get or create CapCut draft
    :param draft_id: Draft ID, if None or corresponding zip file not found, create new draft
    :param width: Video width, default 1080
    :param height: Video height, default 1920
    :return: (draft_id, script)
    """
    global DRAFT_CACHE  # Declare use of global variable

    if not draft_id:
        raise ValueError("draft_id parameter is required and cannot be empty")
    
    if draft_id is not None and draft_id in DRAFT_CACHE:
        # Get existing draft information from cache
        print(f"Getting draft from cache: {draft_id}")
        # Update last access time
        update_cache(draft_id, DRAFT_CACHE[draft_id])
        return draft_id, DRAFT_CACHE[draft_id]

    # Create new draft logic
    print(f"Creating new draft with ID: {draft_id}")
    script, generated_draft_id = create_draft(
        draft_id=draft_id,
        width=width,
        height=height,
    )
    return generated_draft_id, script
    