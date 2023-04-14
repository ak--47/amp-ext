interface Config {
    /**
     * amplitude API key
     */
    api_key: string;
    /**
     * amplitude API secret
     */
    api_secret: string;
    /**
     * start date for data import
     */
    start_date: string;
    /**
     * end date for data import
     */
    end_date: string;
    /**
     * region for data residency
     */
    region?: "US" | "EU";
    /**
     * unit of time to chunk
     */
    time_unit?: "day" | "hour" | "month";
    /**
     * where to put temporary data; must be a valid file path .... default is ./tmp
     */
    tempDir?: string;
    /**
     * where to put extracted data; must be a valid file path .... default is ./amplitude-data
     */
    destDir?: string;
    /**
     * file to write logs; must be a valid file path .... default is ./amplitude-data
     */
    logFile?: string | false;
	/**
	 * log console output messages
	 */
	verbose?: boolean
	/**
	 * remove all temporary + intermediate files
	 */
	cleanup?: boolean

}
