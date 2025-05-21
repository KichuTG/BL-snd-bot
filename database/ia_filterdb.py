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

# Initialize MongoDB client with connection pooling
client = AsyncIOMotorClient(DATABASE_URI, maxPoolSize=50, minPoolSize=10)
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
        indexes = [('$file_name', 'text'), ('$caption', 'text')] if USE_CAPTION_FILTER else [('$file_name', 'text')]
        collection_name = COLLECTION_NAME

async def save_file(media):
    """Save file in database"""
    try:
        file_id, file_ref = unpack_new_file_id(media.file_id)
        # Normalize file_name only once
        file_name = re.sub(r"[-_\.+]+", " ", str(media.file_name)).strip()
        
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
        logger.info(f'{getattr(media, "file_name", "NO_FILE")} saved to database')
        return True, 1
    except ValidationError as e:
        logger.exception(f'Validation error while saving file: {e}')
        return False, 2
    except DuplicateKeyError:
        logger.warning(f'{getattr(media, "file_name", "NO_FILE")} already exists in database')
        return False, 0

async def get_search_results(chat_id, query, file_type=None, max_results=10, offset=0, filter=False):
    """For given query return (results, next_offset, total_results)"""
    # Cache settings to avoid multiple DB calls
    settings = None
    if chat_id is not None:
        settings = await get_settings(int(chat_id))
        try:
            max_results = 10 if settings['max_btn'] else int(MAX_B_TN)
        except KeyError:
            await save_group_settings(int(chat_id), 'max_btn', False)
            max_results = 10 if (await get_settings(int(chat_id)))['max_btn'] else int(MAX_B_TN)

    # Optimize query normalization
    query = query.strip()
    if not query:
        raw_pattern = '.*'
    else:
        # Precompile regex for reuse
        query = re.sub(r'[-:"\';!\s]+', ' ', query).strip()
        raw_pattern = fr'\b{re.escape(query)}\b' if ' ' not in query else query.replace(' ', r'.*[\s]')

    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error:
        logger.error(f"Invalid regex pattern: {raw_pattern}")
        return [], '', 0

    # Build filter with text index support
    mongo_filter = {}
    if USE_CAPTION_FILTER:
        mongo_filter['$text'] = {'$search': query}
    else:
        mongo_filter['file_name'] = {'$regex': regex, '$options': 'i'}

    if file_type:
        mongo_filter['file_type'] = file_type

    try:
        # Use aggregation for efficient counting and fetching
        pipeline = [
            {'$match': mongo_filter},
            {'$sort': {'$natural': -1}},
            {'$facet': {
                'results': [{'$skip': offset}, {'$limit': max_results}],
                'total': [{'$count': 'count'}]
            }}
        ]
        cursor = await Media.aggregate(pipeline).next()
        files = cursor['results']
        total_results = cursor['total'][0]['count'] if cursor['total'] else 0
        next_offset = offset + max_results if offset + max_results < total_results else ''

        return files, next_offset, total_results
    except Exception as e:
        logger.error(f"Error in search: {e}")
        return [], '', 0

async def get_bad_files(query, file_type=None, filter=False):
    """For given query return (results, total_results)"""
    query = query.strip()
    if not query:
        raw_pattern = '.*'
    else:
        query = re.sub(r'[-:"\';!\s]+', ' ', query).strip()
        raw_pattern = fr'\b{re.escape(query)}\b' if ' ' not in query else query.replace(' ', r'.*[\s]')

    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error:
        logger.error(f"Invalid regex pattern: {raw_pattern}")
        return [], 0

    mongo_filter = {}
    if USE_CAPTION_FILTER:
        mongo_filter['$text'] = {'$search': query}
    else:
        mongo_filter['file_name'] = {'$regex': regex, '$options': 'i'}

    if file_type:
        mongo_filter['file_type'] = file_type

    try:
        total_results = await Media.count_documents(mongo_filter)
        cursor = Media.find(mongo_filter).sort('$natural', -1)
        files = await cursor.to_list(length=total_results)
        return files, total_results
    except Exception as e:
        logger.error(f"Error in get_bad_files: {e}")
        return [], 0

async def get_file_details(query):
    """Return file details for a given file_id"""
    try:
        filedetails = await Media.find_one({'file_id': query})
        return [filedetails] if filedetails else []
    except Exception as e:
        logger.error(f"Error fetching file details: {e}")
        return []

def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    """Return file_id, file_ref"""
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
