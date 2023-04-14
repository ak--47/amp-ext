#! /usr/bin/env node

// @ts-check
import u from "ak-tools";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
dayjs.extend(utc);
import fs from 'fs';
import { lstatSync } from 'fs';
import path from "path";
import { got } from "got";
import https from 'https';
import { pipeline } from 'stream/promises';
import { default as zip } from 'adm-zip';
import { default as gun } from 'node-gzip';
import { execSync } from 'child_process';
import esMain from 'es-main';
import yargs from "yargs";

let logText = ``;

/*
----
MAIN
----
*/

/**
 * pulls data out of amplitude
 * @param  {Config} config
 */
async function main(config) {
	const creds = {
		key: config.api_key,
		secret: config.api_secret
	};
	const auth = "Basic " + Buffer.from(creds.key + ":" + creds.secret).toString('base64');

	const {
		start_date,
		end_date,
		verbose = true,
		cleanup = true,
		region = "US",
		time_unit = 'day',
		tempDir = './tmp',
		destDir = './exports',
		logFile = `./logs/amplitude-export-log-${Date.now()}.txt`
	} = config;
	const l = log(verbose);
	l('start\n\nsettings:');
	l({ start_date, end_date, region, verbose, time_unit, tempDir, destDir, logFile });


	const TEMP_DIR = path.resolve(tempDir);
	const DESTINATION_DIR = path.resolve(destDir);
	const LOG_FILE = logFile ? path.resolve(logFile) : false;
	await u.mkdir(TEMP_DIR);
	await u.mkdir(DESTINATION_DIR);

	const start = dayjs.utc(start_date).startOf('day');
	const end = dayjs.utc(end_date).endOf('day');
	const delta = end.diff(start, time_unit);
	const numPairs = Math.ceil(delta) + 1;
	const datePairs = [];
	const dateFormat = 'YYYYMMDDTHH';
	const logFormat = 'YYYY-MM-DDTHH';
	let lastStart = start;

	for (let i = 0; i < numPairs; i++) {
		const pair = {
			start: lastStart.startOf(time_unit).format(dateFormat),
			end: lastStart.endOf(time_unit).format(dateFormat)
		};
		if (pair.start === pair.end) pair.end = lastStart.add(1, time_unit).format(dateFormat);
		datePairs.push(pair);
		lastStart = lastStart.add(1, time_unit);
	}

	datePairs[0].start = start.format(dateFormat);
	datePairs[numPairs - 1].end = end.format(dateFormat);

	// ? https://www.docs.developers.amplitude.com/analytics/apis/export-api/#endpoints
	const url = region === 'US' ? 'https://amplitude.com/api/2/export' : 'https://analytics.eu.amplitude.com/api/2/export';
	l(`\n\n\tDOWNLOAD\n\n`);
	consumeData: for (const dates of datePairs) {
		const logPair = `${dayjs.utc(dates.start).format(logFormat)} → ${dayjs.utc(dates.end).format(logFormat)}`;
		try {
			await pipeline(
				got.stream({
					url,
					searchParams: new URLSearchParams([['start', dates.start], ['end', dates.end]]),
					headers: {
						"Authorization": auth,
						'Connection': 'keep-alive',
					},
					retry: {
						limit: 1000,
						statusCodes: [429, 500, 501, 503, 504, 502],
						errorCodes: [],
						methods: ['GET'],
						noise: 100

					},
					agent: {
						https: new https.Agent({ keepAlive: true })
					},
					hooks: {
						beforeRetry: [(req, resp, count) => {
							try {
								l(`got ${resp?.statusCode}...retrying request...#${count}`);
							}
							catch (e) {
								//noop
							}
						}]
					}

				}),
				fs.createWriteStream(path.resolve(`${TEMP_DIR}/${dates.start}--${dates.end}.zip`))
			);
			l(`${logPair}: SUCCESS 200`);

		}
		catch (e) {
			l(`${logPair}: ${e?.name || ""} ${e?.response?.statusCode || "unknown code"}; ${e?.message || "unknown error"}`);
			continue consumeData;
		}
	}

	const allFiles = (await u.ls(TEMP_DIR)).filter(path => path.endsWith('.zip'));
	const emptyFiles = allFiles.map((path) => {
		return {
			path,
			size: lstatSync(path).size
		};
	}).filter(f => f.size === 0).map(f => f.path);
	for (const empty of emptyFiles) {
		u.rm(empty);
	}
	const downloadedFiles = (await u.ls(TEMP_DIR)).filter(path => path.endsWith('.zip'));

	l(`\n\n\tUNZIP\n\n`);

	unzip: for (const zipFile of downloadedFiles) {
		l(`unzipping ${path.basename(zipFile)}`);
		const fileId = zipFile.split('/').slice().pop().split('.')[0];
		const dir = u.mkdir(path.resolve(`${TEMP_DIR}/${fileId}`));
		try {
			execSync(`unzip -j ${escapeForShell(zipFile)} -d ${escapeForShell(dir)}`);
			if (cleanup) await u.rm(zipFile);
			continue unzip;
		} catch (e) {
			const zipped = new zip(zipFile);
			var zipEntries = zipped.getEntries();
			zipEntries.forEach(function (zipEntry) {
				zipped.extractEntryTo(zipEntry.entryName, `${dir}`, false, true);
			});
			if (cleanup) await u.rm(zipFile);
			continue unzip;
		}
	}

	const folders = (await u.ls(TEMP_DIR)).map(f => {
		return {
			path: f,
			dir: lstatSync(f).isDirectory()
		};
	}).filter(f => f.dir).map(f => f.path);
	const gunzipped = [];

	//@ts-ignore
	for (const folder of folders) {
		const files = (await u.ls(folder)).filter(f => f.endsWith('.json.gz'));
		gunzipped.push(files);
	}

	const writePath = path.resolve(DESTINATION_DIR);
	let eventCount = 0;

	l(`\n\n\tGUNZIP\n\n`);

	ungzip: for (const file of gunzipped.flat()) {
		l(`gunzipping ${path.basename(file)}`);
		const newFileName = path.basename(file).split('_').slice(1).join("_").split('.gz')[0];
		const dest = path.resolve(`${writePath}/${newFileName}`);

		try {
			let source = escapeForShell(file);
			execSync(`gunzip -c ${source} > ${dest}`);
			let numLines = execSync(`wc -l ${dest}`);
			eventCount += Number(numLines.toString().split('/').map(x => x.trim())[0]);
			if (cleanup) await u.rm(file);
			continue ungzip;

		} catch (e) {
			//@ts-ignore
			let dataFile = await u.load(file, false, null);
			//@ts-ignore
			let gunzipped = await gun.ungzip(dataFile);
			let rawData = gunzipped.toString('utf-8');
			let numOfLines = rawData.split('\n').length - 1;
			eventCount += numOfLines;
			await u.touch(dest, rawData);
			if (cleanup) await u.rm(file);
			continue ungzip;
		}
	}

	for (const folder of folders) {
		if (cleanup) await u.rm(folder);
	}
	const extracted = (await u.ls(DESTINATION_DIR)).filter(f => f.endsWith('.json'));
	l(`\nextracted ${u.comma(extracted.length)} files for ${u.comma(eventCount)} events\n`);
	if (LOG_FILE) {
		if (LOG_FILE.includes("/logs/")) await u.mkdir(path.resolve('./logs'));
		await u.touch(path.resolve(LOG_FILE), logText);
	}
	if (cleanup) await u.rm(TEMP_DIR);
	l('\n\nfinish!\n\n');

	return extracted;
}

/*
----
CLI
----
*/

/**
 * @returns {Config}
 */
function cli() {
	if (process?.argv?.slice()?.pop()?.endsWith('.json')) {
		try {
			//@ts-ignore
			const config = JSON.parse(readFileSync(path.resolve(process.argv.slice().pop())));
			return config;
		}
		catch (e) {
			//noop
		}
	}

	const args = yargs(process.argv.splice(2))
		.scriptName("")
		.command('$0', 'usage:\nnpx amp-ext --key foo --secret bar --start 2022-04-20 --end 2023-04-20', () => { })
		.option("api_key", {
			demandOption: true,
			alias: "key",
			describe: 'amplitude API key',
			type: 'string'
		})
		.option("api_secret", {
			demandOption: true,
			alias: "secret",
			describe: 'amplitude API secret',
			type: 'string'
		})
		.option("region", {
			demandOption: false,
			default: "US",
			describe: 'US or EU data residency',
			type: 'string'
		})
		.option("time_unit", {
			demandOption: false,
			alias: "unit",
			default: "month",
			describe: '"day", "month", or "hour" ',
			type: 'string'
		})
		.option("tempDir", {
			demandOption: false,
			default: "./tmp",
			describe: 'where to store downloads',
			type: 'string'
		})
		.option("destDir", {
			demandOption: false,
			default: "./exports",
			describe: 'where to write files',
			type: 'string'
		})
		.option("logFile", {
			demandOption: false,
			alias: "log",
			describe: 'where to write logs',
			type: 'string'
		})
		.option("start_date", {
			demandOption: true,
			alias: 'start',
			describe: 'start date for extract',
			type: 'string'
		})
		.option("end_date", {
			demandOption: true,
			alias: 'end',
			describe: 'end date for extract',
			type: 'string'
		})
		.help()
		.argv;
	//@ts-ignore
	return args;
}

/*
----
LOGGING
----
*/

function log(verbose) {
	return function (data) {
		if (u.isJSON(data)) {
			logText += `${u.json(data)}\n`;
		}
		else {
			logText += `${data}\n`;
		}

		if (verbose) console.log(data);
	};
}

function escapeForShell(arg) {
	return `'${arg.replace(/'/g, `'\\''`)}'`;
}

const hero = String.raw`

  __    _      ___       ____  _    _____ 
 / /\  | |\/| | |_)     | |_  \ \_/  | |  
/_/--\ |_|  | |_|       |_|__ /_/ \  |_|  
	
   the amplitude data vacuum  by AK
`;

/*
----
EXPORTS
----
*/
export default main;

if (esMain(import.meta)) {
	console.log(hero);
	const params = cli();

	main(params)
		.then(() => {
			console.log(`\n\nhooray! all done!\n\n`);
		}).catch((e) => {
			console.log(`\n\nuh oh! something didn't work...\nthe error message is:\n\n\t${e.message}\n\n@\n\n${e.stack}\n\n`);

		}).finally(() => {
			console.log('\n\nhave a great day!\n\n');
			process.exit(0);
		});

}
