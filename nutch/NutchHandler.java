package ash.custom;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.ParseSegment;

public class NutchHandler {

	public static void main(String[] args) {
		String url_dir = "/Users/ash/Documents/workspace/nutch/urls";
		String crawldb_dir = "/Users/ash/Documents/workspace/nutch/crawldb";
		String segment_dir = crawldb_dir+"/segment";
		
		
		String args1[] = {crawldb_dir,url_dir};
		String args2[] = {crawldb_dir, segment_dir};
		
		try {
			Injector.customInject(args1);
			Generator.customGenerate(args2);
			
			File directory = new File(segment_dir);
			String[] list = directory.list();
			Arrays.sort(list);
			String lastFolder = list[list.length-1];

			String args3[] = {segment_dir+"/"+lastFolder};

			Fetcher.customFetch(args3);
			ParseSegment.customParse(args3);
			String args4[] = {crawldb_dir,"-dir",segment_dir};
			
			CrawlDb.customUpdateDB(args4);
						
			IndexingJob.main(args4);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
