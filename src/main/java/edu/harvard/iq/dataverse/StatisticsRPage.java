/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.dataverse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.ejb.EJB;
import javax.faces.context.FacesContext;
import javax.faces.view.ViewScoped;
import javax.inject.Inject;
import javax.inject.Named;

import edu.harvard.iq.dataverse.DatasetVersionServiceBean.RetrieveDatasetVersionResponse;
import edu.harvard.iq.dataverse.authorization.AuthenticationServiceBean;
import edu.harvard.iq.dataverse.dataaccess.DataAccess;
import edu.harvard.iq.dataverse.dataaccess.StorageIO;
import edu.harvard.iq.dataverse.datasetutility.WorldMapPermissionHelper;
import edu.harvard.iq.dataverse.export.ExportException;
import edu.harvard.iq.dataverse.export.ExportService;
import edu.harvard.iq.dataverse.export.spi.Exporter;
import edu.harvard.iq.dataverse.externaltools.ExternalTool;
import edu.harvard.iq.dataverse.externaltools.ExternalToolServiceBean;
import edu.harvard.iq.dataverse.makedatacount.MakeDataCountLoggingServiceBean;
import edu.harvard.iq.dataverse.makedatacount.MakeDataCountLoggingServiceBean.MakeDataCountEntry;
import edu.harvard.iq.dataverse.rserve.RConvertSavToCsv;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.FileUtil;
import edu.harvard.iq.dataverse.util.SystemConfig;

/**
 *
 * @author skraffmi
 * 
 */

@ViewScoped
@Named("StatisticsRPage")
public class StatisticsRPage implements java.io.Serializable {

    private String csvFilePath;

    private FileMetadata fileMetadata;
    private Long fileId;
    private String version;
    private Boolean exists;
    private DataFile file;
    private int selectedTabIndex;
    private Dataset editDataset;
    private List<DatasetVersion> datasetVersionsForTab;
    private List<FileMetadata> fileMetadatasForTab;
    private String persistentId;
    private List<ExternalTool> configureTools;
    private List<ExternalTool> exploreTools;
    private String rowVariables;
    private String colVariables;
    private String valVariables;
    private Map<String,String> sum;
    private String variable1;
    private String folderSystemPath;

    @EJB
    DataFileServiceBean datafileService;

    @EJB
    DatasetVersionServiceBean datasetVersionService;

    @EJB
    PermissionServiceBean permissionService;
    @EJB
    SettingsServiceBean settingsService;
    @EJB
    FileDownloadServiceBean fileDownloadService;
    @EJB
    GuestbookResponseServiceBean guestbookResponseService;
    @EJB
    AuthenticationServiceBean authService;

    @EJB
    SystemConfig systemConfig;

    @Inject
    DataverseSession session;
    @EJB
    EjbDataverseEngine commandEngine;
    @EJB
    ExternalToolServiceBean externalToolService;
    @EJB
    RConvertSavToCsv rconvertService;

    @Inject
    DataverseRequestServiceBean dvRequestService;
    @Inject
    PermissionsWrapper permissionsWrapper;
    @Inject
    FileDownloadHelper fileDownloadHelper;
    @Inject
    WorldMapPermissionHelper worldMapPermissionHelper;
    @Inject
    MakeDataCountLoggingServiceBean mdcLogService;

    private static final Logger logger = Logger.getLogger(StatisticsRPage.class.getCanonicalName());

    private boolean fileDeleteInProgress = false;

    public String init() {
        this.setExists(false);

        if (fileId != null || persistentId != null) {

            // ---------------------------------------
            // Set the file and datasetVersion
            // ---------------------------------------
            if (fileId != null) {
                file = datafileService.find(fileId);

            } else if (persistentId != null) {
                file = datafileService.findByGlobalId(persistentId);
                if (file != null) {
                    fileId = file.getId();
                }

            }
            if (file == null || fileId == null) {
                return permissionsWrapper.notFound();
            }

            // Is the Dataset harvested?
            if (file.getOwner().isHarvested()) {
                // if so, we'll simply forward to the remote URL for the original
                // source of this harvested dataset:
                String originalSourceURL = file.getOwner().getRemoteArchiveURL();
                if (originalSourceURL != null && !originalSourceURL.equals("")) {
                    logger.fine("redirecting to " + originalSourceURL);
                    try {
                        FacesContext.getCurrentInstance().getExternalContext().redirect(originalSourceURL);
                    } catch (IOException ioex) {
                        // must be a bad URL...
                        // we don't need to do anything special here - we'll redirect
                        // to the local 404 page, below.
                        logger.warning("failed to issue a redirect to " + originalSourceURL);
                    }
                }

                return permissionsWrapper.notFound();
            }

            RetrieveDatasetVersionResponse retrieveDatasetVersionResponse;
            retrieveDatasetVersionResponse = datasetVersionService.selectRequestedVersion(file.getOwner().getVersions(),
                    version);
            Long getDatasetVersionID = retrieveDatasetVersionResponse.getDatasetVersion().getId();

            fileMetadata = datafileService.findFileMetadataByDatasetVersionIdAndDataFileId(getDatasetVersionID, fileId);

            String folderPathString = fileMetadata.getDatasetVersion().getDataset().getStorageIdentifier(); // file://....

            try {
                StorageIO directStorageAccess = DataAccess.getDirectStorageIO(folderPathString);// get the full path
                                                                                                // without filename

                //Get the Columns of the File
                this.setFolderSystemPath(System.getProperty("dataverse.files.directory") + File.separator
                + directStorageAccess.getFileSystemPath().toString());
                //this.setSum(rconvertService.getRStatistics(folderSystemPath, file.getStorageIdentifier() + ".orig", "q34"));
                /*if (file.getDataTable().getOriginalFileFormat().equals("application/x-spss-sav")) {
                    String folderSystemPath = System.getProperty("dataverse.files.directory") + File.separator
                            + directStorageAccess.getFileSystemPath().toString();

                    String fileSystemPath = System.getProperty("dataverse.files.directory") + File.separator
                            + directStorageAccess.getFileSystemPath().toString() + File.separator
                            + file.getStorageIdentifier();
                    logger.severe("fileSystemPath." + fileSystemPath + " folderSystemPath" + folderSystemPath);
                    logger.severe("original file format: " + file.getDataTable().getOriginalFileFormat());
                    // check if .csv file already exists
                    File f = new File(fileSystemPath + ".csv");
                    if (f.exists() && !f.isDirectory()) {
                        this.setExists(true);
                    }
                    // if it doesnt exist then create it
                    else {
                        rconvertService.convertFile(folderSystemPath, file.getStorageIdentifier());
                        // we have put it on temp directory - now we have to copy it to the appropriate
                        // folder
                        // TODO tmp Folder from properties
                        FileUtil.copyFile(new File("/tmp/" + file.getStorageIdentifier() + ".csv"),
                                new File(fileSystemPath + ".csv"));
                    }

                    this.setCsvFilePath(File.separator + "files" + File.separator
                            + directStorageAccess.getFileSystemPath().toString() + File.separator
                            + file.getStorageIdentifier() + ".csv");
                } else if (file.getDataTable().getOriginalFileFormat().equals("text/csv")) {
                    this.setCsvFilePath(File.separator + "files" + File.separator
                            + directStorageAccess.getFileSystemPath().toString() + File.separator
                            + file.getStorageIdentifier() + ".orig");
                }*/
                
                // this.prepCsvFile(this.getCsvFilePath());
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            if (fileMetadata == null) {
                logger.fine("fileMetadata is null! Checking finding most recent version file was in.");
                fileMetadata = datafileService.findMostRecentVersionFileIsIn(file);
                if (fileMetadata == null) {
                    return permissionsWrapper.notFound();
                }
            }

            // If this DatasetVersion is unpublished and permission is doesn't have
            // permissions:
            // > Go to the Login page
            //
            // Check permisisons
            Boolean authorized = (fileMetadata.getDatasetVersion().isReleased())
                    || (!fileMetadata.getDatasetVersion().isReleased() && this.canViewUnpublishedDataset());

            if (!authorized) {
                return permissionsWrapper.notAuthorized();
            }

            this.guestbookResponseService.initGuestbookResponseForFragment(fileMetadata, session);

            if (fileMetadata.getDatasetVersion().isPublished()) {
                MakeDataCountEntry entry = new MakeDataCountEntry(FacesContext.getCurrentInstance(), dvRequestService,
                        fileMetadata.getDatasetVersion());
                mdcLogService.logEntry(entry);
            }

            // Find external tools based on their type, the file content type, and whether
            // ingest has created a derived file for that type
            // Currently, tabular data files are the only type of derived file created, so
            // isTabularData() works - true for tabular types where a .tab file has been
            // created and false for other mimetypes
            String contentType = file.getContentType();

            // For tabular data, indicate successful ingest by returning a contentType for
            // the derived .tab file
            if (file.isTabularData()) {
                contentType = DataFileServiceBean.MIME_TYPE_TSV_ALT;
            }
            configureTools = externalToolService.findByType(ExternalTool.Type.CONFIGURE, contentType);
            exploreTools = externalToolService.findByType(ExternalTool.Type.EXPLORE, contentType);

        } else {

            return permissionsWrapper.notFound();
        }

        return null;
    }

    private boolean canViewUnpublishedDataset() {
        return permissionsWrapper.canViewUnpublishedDataset(dvRequestService.getDataverseRequest(),
                fileMetadata.getDatasetVersion().getDataset());
    }

    public void onVariableChange() {
       
                this.setSum(rconvertService.getRStatistics(this.getFolderSystemPath(), file.getStorageIdentifier() + ".orig", variable1, file.getDataTable().getOriginalFileFormat()));
    }

    public FileMetadata getFileMetadata() {
        return fileMetadata;
    }

    public Boolean getExists() {
        return exists;
    }

    public String getRowVariables() {
        return this.rowVariables;
    }

    public String getColVariables() {
        return this.colVariables;
    }

    public String getValVariables() {
        return this.valVariables;
    }

    public String getCsvFilePath() {
        return csvFilePath;
    }

    public void setFileMetadata(FileMetadata fileMetadata) {
        this.fileMetadata = fileMetadata;
    }

    public void setExists(Boolean exists) {
        this.exists = exists;
    }

    public String transformVariables(String[] variables) {
        String tempVar = "[";
        for (int i = 0; i < variables.length; i++)
            tempVar += "\"" + variables[i] + "\",";
        tempVar = tempVar.subSequence(0, tempVar.length() - 1).toString() + "]";
        return tempVar;
    }

    public void setRowVariables(String[] rowVariables) {

        this.rowVariables = transformVariables(rowVariables);
    }

    public void setColVariables(String[] colVariables) {

        this.colVariables = transformVariables(colVariables);
    }

    public void setValVariables(String[] valVariables) {

        this.valVariables = transformVariables(valVariables);
    }

    public void setCsvFilePath(String csvFilePath) {
        this.csvFilePath = csvFilePath;
    }

    public DataFile getFile() {
        return file;
    }

    public void setFile(DataFile file) {
        this.file = file;
    }

    public Long getFileId() {
        return fileId;
    }

    public void setFileId(Long fileId) {
        this.fileId = fileId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
    
    private int activeTabIndex;

    public int getActiveTabIndex() {
        return activeTabIndex;
    }

    public void setActiveTabIndex(int activeTabIndex) {
        this.activeTabIndex = activeTabIndex;
    }

    public List<FileMetadata> getFileMetadatasForTab() {
        return fileMetadatasForTab;
    }

    public void setFileMetadatasForTab(List<FileMetadata> fileMetadatasForTab) {
        this.fileMetadatasForTab = fileMetadatasForTab;
    }

    public String getPersistentId() {
        return persistentId;
    }

    public void setPersistentId(String persistentId) {
        this.persistentId = persistentId;
    }

    public List<DatasetVersion> getDatasetVersionsForTab() {
        return datasetVersionsForTab;
    }

    public void setDatasetVersionsForTab(List<DatasetVersion> datasetVersionsForTab) {
        this.datasetVersionsForTab = datasetVersionsForTab;
    }

    private Boolean thumbnailAvailable = null;

    public boolean isThumbnailAvailable(FileMetadata fileMetadata) {
        // new and optimized logic:
        // - check download permission here (should be cached - so it's free!)
        // - only then ask the file service if the thumbnail is available/exists.
        // the service itself no longer checks download permissions.
        // (Also, cache the result the first time the check is performed...
        // remember - methods referenced in "rendered=..." attributes are
        // called *multiple* times as the page is loading!)

        if (thumbnailAvailable != null) {
            return thumbnailAvailable;
        }

        if (!fileDownloadHelper.canDownloadFile(fileMetadata)) {
            thumbnailAvailable = false;
        } else {
            thumbnailAvailable = datafileService.isThumbnailAvailable(fileMetadata.getDataFile());
        }

        return thumbnailAvailable;
    }

    public int getSelectedTabIndex() {
        return selectedTabIndex;
    }

    public void setSelectedTabIndex(int selectedTabIndex) {
        this.selectedTabIndex = selectedTabIndex;
    }

    public Map<String, String> getSum() {
        return sum;
    }

    public void setSum(Map<String, String> sum) {
        this.sum = sum;
    }

    public String getVariable1() {
        return variable1;
    }

    public void setVariable1(String variable1) {
        this.variable1 = variable1;
    }

    public String getFolderSystemPath() {
        return folderSystemPath;
    }

    public void setFolderSystemPath(String folderSystemPath) {
        this.folderSystemPath = folderSystemPath;
    }



}
