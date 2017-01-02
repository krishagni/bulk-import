package com.krishagni.importer.services.impl;

public class ImporterContextHolder {
    private static ImporterContextHolder instance = new ImporterContextHolder();

    //
    // Thread specific contextual information
    // Stores boolean indicating whether current thread is
    // doing import operation or not.
    //
    private static ThreadLocal<Boolean> importerCtx = null;

    private ImporterContextHolder() {
    }


    public static ImporterContextHolder getInstance() {
        return instance;
    }

    public void newContext() {
        importerCtx = new ThreadLocal<Boolean>() {
            protected Boolean initialValue() {
                return true;
            }
        };
    }

    public void clearContext() {
        importerCtx.remove();
        importerCtx = null;
    }

    public boolean isImportOp() {
        return importerCtx != null && importerCtx.get();
    }
}
