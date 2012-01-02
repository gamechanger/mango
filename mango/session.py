import datetime
from django.contrib.sessions.backends.base import SessionBase, CreateError
from django.utils.encoding import force_unicode
from mango import database as db, OperationFailure, collection
import os
import uuid

class SessionStore(SessionBase):
    """
    Implements MongoDB session store.
    """
    def load(self):
        s = db[collection].find_one({'_id': self.session_key})

        if not s:
            return {}

        if s and s['expire_date'] < datetime.datetime.now():
            self.delete()
            return {}

        try:
            # in case of bad data, bail out (this is possible)
            return self.decode(force_unicode(s['session_data']))
        except Exception as e:
            self.delete()
            return {}

    def session_data(self):
        s = db[collection].find_one({'_id': self.session_key})
        if s:
            return s['session_data']


    def exists(self, session_key):
        return True if db[collection].find_one({'_id': session_key}) else False

    def create(self):
        while True:
            self.session_key = self._get_new_session_key()
            try:
                # Save immediately to ensure we have a unique entry in the
                # database.
                self.save(must_create=True)
            except CreateError:
                # Key wasn't unique. Try again.
                continue
            self.modified = True
            self._session_cache = {}
            return

    def save(self, must_create=False):
        """
        Saves the current session data to the database. If 'must_create' is
        True, a database error will be raised if the saving operation doesn't
        create a *new* entry (as opposed to possibly updating an existing
        entry).
        """
        obj = {'_id': self.session_key,
               'session_data': self.encode(self._get_session(no_load=must_create)),
               'expire_date': self.get_expiry_date()}

        try:
            # always pass safe=True, so that we don't drop sessions
            if must_create:
                db[collection].insert(obj, safe=True)
            else:
                db[collection].update({'_id': self.session_key},
                                      obj, upsert=True, safe=True)
        except OperationFailure, e:
            if must_create:
                raise CreateError
            raise e

    def delete(self, session_key=None):
        if session_key is None:
            if self._session_key is None:
                return
            session_key = self._session_key
        db[collection].remove({'_id': session_key})

    def _get_new_session_key(self):
        """
        This method uses a sharding-friendly session key
        """
        while 1:
            import os
            guid = str(uuid.UUID(bytes=os.urandom(16), version=4)).replace('-','')
            session_key = "%s%s"% (datetime.datetime.now().strftime("%y%m%d"), guid[-26:])
            if not self.exists(session_key):
                break
        return session_key

