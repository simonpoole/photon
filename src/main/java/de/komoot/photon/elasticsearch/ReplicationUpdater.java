package de.komoot.photon.elasticsearch;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cedarsoftware.util.io.JsonWriter;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.elasticsearch.PhotonAction.ACTION;
import de.komoot.photon.utils.FileSequenceFormatter;
import lombok.extern.slf4j.Slf4j;

/**
 * Generate replication files from updates, a further updater can be daisy-chained
 * 
 * @author Simon
 *
 */
@Slf4j
public class ReplicationUpdater implements de.komoot.photon.Updater {

    private static final String STATE_FILE_SUFFIX = ".state.json";

    public static final String REPLICATION_FORMAT = "0.0.0";

    final de.komoot.photon.Updater otherUpdater;
    final File                     baseDirectory;
    List<PhotonAction>             actions = new ArrayList<>();
    long                           sequenceNumber;
    final FileSequenceFormatter    formatter;
    final Path                     stateFilePath;

    /**
     * Construct a new ReplicationUpdater
     * 
     * @param baseDirectory the directory in which we store the replication files
     * @param otherUpdater a further updater to call
     */
    public ReplicationUpdater(@Nonnull final File baseDirectory, @Nullable final de.komoot.photon.Updater otherUpdater) {
        this.otherUpdater = otherUpdater;
        this.baseDirectory = baseDirectory;
        formatter = new FileSequenceFormatter(baseDirectory);
        stateFilePath = Paths.get(baseDirectory + "/state.json");
        try {
            ReplicationState initialState = ReplicationState.readState(stateFilePath);
            sequenceNumber = initialState.getSequenceNumber();
            sequenceNumber++;
        } catch (IOException e) {
            sequenceNumber = 0;
        }
        log.info(String.format("Next sequence number %d", sequenceNumber));
    }

    @Override
    public void create(PhotonDoc doc) {
        if (otherUpdater != null) {
            otherUpdater.create(doc);
        }
        addAction(actions, doc.getUid(), ACTION.CREATE, doc);
    }

    @Override
    public void update(PhotonDoc doc) {
        if (otherUpdater != null) {
            otherUpdater.update(doc);
        }
        addAction(actions, doc.getUid(), ACTION.UPDATE, doc);
    }

    @Override
    public void delete(String docId) {
        if (otherUpdater != null) {
            otherUpdater.delete(docId);
        }
        addAction(actions, docId, ACTION.DELETE, null);
    }

    @Override
    public void delete(String osmType, long osmId, String osmKey, String osmValue) {
        if (otherUpdater != null) {
            otherUpdater.delete(osmType, osmId, osmKey, osmValue);
        }
        addDeleteOsmAction(actions, osmType, osmId, osmKey, osmValue);
    }

    @Override
    public void updateOrCreate(PhotonDoc updatedDoc) {
        if (otherUpdater != null) {
            otherUpdater.updateOrCreate(updatedDoc);
        }
        addAction(actions, updatedDoc.getUid(), ACTION.UPDATE_OR_CREATE, updatedDoc);
    }

    @Override
    public void finish() {
        if (otherUpdater != null) {
            otherUpdater.finish();
        }
        if (actions.isEmpty()) {
            log.warn("Update empty");
            return;
        }
        String json = JsonWriter.objectToJson(actions);
        try (PrintStream outputStream = new PrintStream(
                new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(formatter.getFormattedName(sequenceNumber, ".json.gz")))), false,
                "UTF-8");) {
            outputStream.print(json);
            saveState(REPLICATION_FORMAT);
            sequenceNumber++;
            actions.clear();
        } catch (IOException ioex) {
            log.error("Exception writing replication file ", ioex);
            throw new IOError(ioex); // we need to abort here or else updates could be lost
        }
    }

    /**
     * Save the current state in a per-sequence id file and then link the global state file to it
     * 
     * @param replicationFormat the current replication format version
     */
    private void saveState(@Nonnull String replicationFormat) {
        final ReplicationState state = new ReplicationState(sequenceNumber, replicationFormat, new Date());
        PrintStream outputStream = null;
        try {
            File newStateFile = formatter.getFormattedName(sequenceNumber, STATE_FILE_SUFFIX);
            outputStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(formatter.getFormattedName(sequenceNumber, STATE_FILE_SUFFIX))));
            outputStream.print(state.toJsonString());
            Files.deleteIfExists(stateFilePath);
            Files.createLink(stateFilePath, newStateFile.toPath());
        } catch (IOException ioex) {
            log.error("Exception writing replication state file ", ioex);
            throw new IOError(ioex);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    /**
     * Add an action and a PhotonDoc to the list of updates
     * 
     * @param actions the List of PhotonActions
     * @param docId the document id for the document
     * @param actionType the action to perform
     * @param doc the PhotonDoc or null if the action is delete
     */
    private void addAction(@Nonnull List<PhotonAction> actions, @Nonnull String docId, @Nonnull ACTION actionType, @Nullable PhotonDoc doc) {
        PhotonAction action;
        action = new PhotonAction();
        action.action = actionType;
        action.id = docId;
        action.doc = doc;
        actions.add(action);
    }

    /**
     * Add an action and a PhotonDoc to the list of updates
     * 
     * @param actions the List of PhotonActions
     * @param osmType the type of OSM element
     * @param osmId the OSM id of the element
     * @param osmKey optional tag key
     * @param osmValue optional tag value
     */
    private void addDeleteOsmAction(@Nonnull List<PhotonAction> actions, @Nonnull String osmType, long osmId, @Nullable String osmKey,
            @Nullable String osmValue) {
        PhotonAction action;
        action = new PhotonAction();
        action.action = ACTION.DELETE_OSM;
        action.osmType = osmType;
        action.osmId = osmId;
        action.osmKey = osmKey;
        action.osmValue = osmValue;
        actions.add(action);
    }
}
