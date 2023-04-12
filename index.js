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


	const { start_date, end_date, time_unit = 'day', tempDir = './tmp', destDir = './amplitude-data' } = config;
	const TEMP_DIR = path.resolve(tempDir);
	const DESTINATION_DIR = path.resolve(destDir);
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

	datePairs[0].start = start.format(dateFormat)
	datePairs[numPairs-1].end = end.format(dateFormat)

	// ? https://www.docs.developers.amplitude.com/analytics/apis/export-api/#endpoints
	const url = config.region === 'US' ? 'https://amplitude.com/api/2/export' : 'https://analytics.eu.amplitude.com/api/2/export';
	consumeData: for (const dates of datePairs) {
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
								l(`got ${resp.message}...retrying request...#${count}`);
							}
							catch (e) {
								//noop
							}
						}]
					}

				}),
				fs.createWriteStream(path.resolve(`./tmp/${dates.start}--${dates.end}.zip`))
			);
			l(`${dayjs.utc(dates.start).format(logFormat)} → ${dayjs.utc(dates.end).format(logFormat)}: got 200; OK`);

		}
		catch (e) {
			l(`${dayjs.utc(dates.start).format(logFormat)} → ${dayjs.utc(dates.end).format(logFormat)}: got ${e.response.statusCode}; NOT FOUND`);
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

	unzip: for (const zipFile of downloadedFiles) {
		const fileId = zipFile.split('/').slice().pop().split('.')[0];
		const dir = u.mkdir(path.resolve(`${TEMP_DIR}/${fileId}`));
		try {
			execSync(`unzip -j ${escapeForShell(zipFile)} -d ${escapeForShell(dir)}`);
			await u.rm(zipFile);
			continue unzip;
		} catch (e) {
			const zipped = new zip(zipFile);
			var zipEntries = zipped.getEntries();
			zipEntries.forEach(function (zipEntry) {
				zipped.extractEntryTo(zipEntry.entryName, `${dir}`, false, true);
			});
			await u.rm(zipFile);
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

	ungzip: for (const file of gunzipped.flat()) {
		const newFileName = path.basename(file).split('_').slice(1).join("_").split('.gz')[0];
		const dest = path.resolve(`${writePath}/${newFileName}`);

		try {
			let source = escapeForShell(file);
			execSync(`gunzip -c ${source} > ${dest}`);
			let numLines = execSync(`wc -l ${dest}`);
			eventCount += Number(numLines.toString().split('/').map(x => x.trim())[0]);
			await u.rm(file);
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
			await u.rm(file);
			continue ungzip;
		}
	}

	for (const folder of folders) {
		await u.rm(folder);
	}
	const extracted = (await u.ls(DESTINATION_DIR)).filter(f => f.endsWith('.json'));
	l(`\nextracted ${u.comma(extracted.length)} files for ${u.comma(eventCount)} events\n`);


	return extracted;
}
/**
 * @returns {Config}
 */
function cli() {
	const args = yargs(process.argv.splice(2))
		.scriptName("amplitude-extract")
		.command('$0', 'extract data from amplitude', () => { })
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
			default: "./amplitude-data",
			describe: 'where to write files',
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

function l(data) {
	console.log(data);
}

function escapeForShell(arg) {
	return `'${arg.replace(/'/g, `'\\''`)}'`;
}

export default main;

if (esMain(import.meta)) {
	const params = cli();

	main(params)
		.then(() => {
			//noop
		}).catch((e) => {
			l(`\nuh oh! something didn't work...\nthe error message is:\n\n\t${e.message}\n\n`);

		}).finally(() => {
			l('\n\nhave a great day!\n\n');
			process.exit(0);
		});

}
