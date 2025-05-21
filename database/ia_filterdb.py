import asyncio
import logging
from struct import pack
import re
import base64
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow.exceptions import ValidationError
from info import DATABASE_URI, DATABASE_NAME, COLLECTION_NAME, USE_CAPTION_FILTER, MAX_B_TN
from utils import get_settings, save_group_settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize database connection and indexes once at startup
client = AsyncIOMotorClient(DATABASE_URI, maxPoolSize=100)  # Increased connection pool size
db = client[DATABASE_NAME]
instance = Instance.from_db(db)

@instance.register
class Media(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)

    class Meta:
        indexes = ('$file_name', 'file_type')  # Added file_type to indexes for faster filtering
        collection_name = COLLECTION_NAME

# Cache for compiled regex patterns to avoid recompilation
_regex_cache = {}

async def save_file(media):
    """Save file in database with optimized duplicate handling"""
    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"(_|\-|\.|\+)", " ", str(media.file_name))
    
    # Check for existing file first to avoid ValidationError overhead
    existing = await Media.find_one({'_id': file_id})
    if existing:
        logger.warning(f'{getattr(media, "file_name", "NO_FILE")} is already saved in database')
        return False, 0
    
    try:
        file = Media(
            file_id=file_id,
            file_ref=file_ref,
            file_name=file_name,
            file_size=media.file_size,
            file_type=media.file_type,
            mime_type=media.mime_type,
            caption=media.caption.html if media.caption else None,
        )
        await file.commit()
    except (ValidationError, DuplicateKeyError):
        logger.exception('Error occurred while saving file in database')
        return False, 2
    else:
        logger.info(f'{getattr(media, "file_name", "NO_FILE")} is saved to database')
        return True, 1

async def get_search_results(chat_id, query, file_type=None, max_results=10, offset=0, filter=False):
    """Optimized search with cached regex and better query construction"""
    # Get settings with single database call
    if chat_id is not None:
        settings = await get_settings(int(chat_id))
        max_results = 10 if settings.get('max_btn', False) else int(MAX_B_TN)
    
    # Query processing with caching
    query = re.sub(r"[-:\"';!]", " ", query).strip()
    query = re.sub(r"\s+", " ", query)
    
    # Use cached regex if available
    cache_key = f"{query}_{file_type}"
    if cache_key in _regex_cache:
        regex, filter_query = _regex_cache[cache_key]
    else:
        if not query:
            raw_pattern = '.'
        elif ' ' not in query:
            raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
        else:
            raw_pattern = query.replace(' ', r'.*[\s\.\+\-_]')
        
        try:
            regex = re.compile(raw_pattern, flags=re.IGNORECASE)
        except:
            return [], '', 0
        
        # Build filter query once
        if USE_CAPTION_FILTER:
            filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}
        else:
            filter_query = {'file_name': regex}
        
        if file_type:
            filter_query['file_type'] = file_type
        
        _regex_cache[cache_key] = (regex, filter_query)
    
    # Get total count and results in parallel
    total_results, files = await asyncio.gather(
        Media.count_documents(filter_query),
        Media.find(filter_query)
            .sort('$natural', -1)
            .skip(offset)
            .limit(max_results)
            .to_list(length=max_results)
    )
    
    next_offset = offset + max_results if offset + max_results < total_results else ''
    
    return files, next_offset, total_results

async def get_bad_files(query, file_type=None, filter=False):
    """Optimized bad files search with caching"""
    query = query.strip()
    
    # Use cached regex if available
    cache_key = f"bad_{query}_{file_type}"
    if cache_key in _regex_cache:
        regex, filter_query = _regex_cache[cache_key]
    else:
        if not query:
            raw_pattern = '.'
        elif ' ' not in query:
            raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
        else:
            raw_pattern = query.replace(' ', r'.*[\s\.\+\-_]')
        
        try:
            regex = re.compile(raw_pattern, flags=re.IGNORECASE)
        except:
            return [], 0
        
        if USE_CAPTION_FILTER:
            filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}
        else:
            filter_query = {'file_name': regex}
        
        if file_type:
            filter_query['file_type'] = file_type
        
        _regex_cache[cache_key] = (regex, filter_query)
    
    total_results = await Media.count_documents(filter_query)
    files = await Media.find(filter_query).sort('$natural', -1).to_list(length=total_results)
    
    return files, total_results

async def get_file_details(query):
    """Optimized single file lookup"""
    return await Media.find_one({'file_id': query})

def encode_file_id(s: bytes) -> str:
    """Optimized file ID encoding"""
    r = bytearray()
    n = 0

    for i in s + bytes([22, 4]):
        if i == 0:
            n += 1
        else:
            if n:
                r.extend(b"\x00" + bytes([n]))
                n = 0
            r.append(i)

    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    """Optimized file ID unpacking"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref
