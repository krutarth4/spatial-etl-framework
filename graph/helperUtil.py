import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def get_tag(tags, key: str) -> Optional[str]:
    """
    Equivalent of Java's:
        public static Optional<Tag> getTag(Collection<Tag> tags, String key)

    Works with:
        - pyosmium.osm.TagList
        - dict-like mappings (e.g., relation.tags)
        - list[Tag] where Tag has .k and .v attributes
    """
    if tags is None:
        return None

    # Case 1: pyosmium dict-like tag list
    if isinstance(tags, dict):
        return tags.get(key)

    # Case 2: iterable of (k, v)
    try:
        for k, v in tags.items():
            if k == key:
                return v
    except AttributeError:
        pass

    # Case 3: list of Tag-like objects (with .k / .key)
    try:
        for tag in tags:
            tag_key = getattr(tag, "k", getattr(tag, "key", None))
            tag_val = getattr(tag, "v", getattr(tag, "value", None))
            if tag_key == key:
                return tag_val
    except Exception as e:
        logger.debug(f"get_tag: failed to read tags: {e}")

    return None


def get_tags_key_starts_with(prefix: str, tags) -> Dict[str, str]:
    """
    Equivalent of:
        public static List<Tag> getTagsKeyStartsWith(String prefix, Collection<Tag> tags, boolean includePrefix)

    Returns a dict of all (key, value) pairs where key starts with the prefix.
    Works with pyosmium mappings or Tag collections.
    """
    result = {}

    if tags is None:
        return result

    # Case 1: pyosmium TagList behaves like dict
    if isinstance(tags, dict):
        for k, v in tags.items():
            if k.lower().startswith(prefix.lower()):
                result[k] = v
        return result

    # Case 2: Tag-like objects
    try:
        for tag in tags:
            tag_key = getattr(tag, "k", getattr(tag, "key", None))
            tag_val = getattr(tag, "v", getattr(tag, "value", None))
            if tag_key and tag_key.lower().startswith(prefix.lower()):
                result[tag_key] = tag_val
    except Exception as e:
        logger.debug(f"get_tags_key_starts_with: failed to parse tags: {e}")

    return result
