package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.print.Doc;
import java.text.MessageFormat;
import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    //TODO> Ticket: User Management - do the necessary changes so that the sessions collection
    //returns a Session object
    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        //TODO> Ticket: User Management - implement the necessary changes so that the sessions
        // collection returns a Session objects instead of Document objects.
        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        try {
            //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!
            usersCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(user);
            return true;
        } catch (MongoWriteException | IncorrectDaoOperation ex) {
            log.error("Could not insert `{}` into `users` collection: {}", user.getEmail(), ex.getMessage());
            throw new IncorrectDaoOperation(
                    MessageFormat.format("User with email `{0}` already exists", user.getEmail()));
        }
        //TODO > Ticket: Handling Errors - make sure to only add new users
        // and not users that already exist.

    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        //TODO> Ticket: User Management - implement the method that allows session information to be
        // stored in it's designated collection.
        UpdateOptions options = new UpdateOptions();
        options.upsert(true);
        try {
            UpdateResult resultWithUpsert = sessionsCollection.updateOne(Filters.eq("user_id",  userId),
                    Updates.set("jwt", jwt),
                    options);
            return true;
        } catch (MongoWriteException e) {
            String errorMessage =
                    MessageFormat.format(
                            "Unable to $set jwt token in sessions collection: {}", e.getMessage());
            throw new IncorrectDaoOperation(errorMessage, e);
        }
        //TODO > Ticket: Handling Errors - implement a safeguard against
        // creating a session with the same jwt token.
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {

        User user = usersCollection.find(new Document("email", email)).limit(1).first();
        //TODO> Ticket: User Management - implement the query that returns the first User object.
        return user;
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        //TODO> Ticket: User Management - implement the method that returns Sessions for a given
        // userId
        return sessionsCollection.find(Filters.eq("user_id", userId), Session.class).limit(1).iterator().tryNext();
    }

    public boolean deleteUserSessions(String userId) {
        //TODO> Ticket: User Management - implement the delete user sessions method
        try {
            return sessionsCollection.deleteOne(new Document("user_id", userId)).getDeletedCount() == 1;
        } catch (MongoWriteException ex) {
            return false;
        }
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        // remove user sessions
        //TODO> Ticket: User Management - implement the delete user method
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions.
        try {
            DeleteResult userDel = usersCollection.deleteOne(Filters.eq("email", email));
            DeleteResult sessionDel = sessionsCollection.deleteOne(Filters.eq("user_id", email));
            if(userDel.getDeletedCount() >= sessionDel.getDeletedCount()) {
                log.warn("User with `email` {} not found. Potential concurrent operation?!");
            }
            return userDel.wasAcknowledged();
        } catch (MongoWriteException ex) {
            String errorMessage = MessageFormat.format("Issue caught while trying to " +
                            "delete user `{}`: {}",
                    email,  ex.getMessage());
            throw new IncorrectDaoOperation(errorMessage);
        }
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        //TODO> Ticket: User Preferences - implement the method that allows for user preferences to
        // be updated.
        try {
            if(userPreferences == null) {
                throw new IncorrectDaoOperation("Preferences can't be null");
            }
            usersCollection.updateOne(Filters.eq("email", email),
                Updates.set("preferences", userPreferences));
            return true;
        } catch (MongoException ex) {
            String errorMessage =
                    MessageFormat.format("Issue caught while trying to update user `{}`: {}",
                            email, ex.getMessage());
            throw new IncorrectDaoOperation(errorMessage);
        }
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions when updating an entry.
    }
}
