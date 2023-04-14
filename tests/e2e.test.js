/* cSpell:disable */
// @ts-nocheck
/* eslint-disable no-undef */
/* eslint-disable no-debugger */
/* eslint-disable no-unused-vars */
import main from "../index.js";
import dotenv from 'dotenv';
import { execSync } from "child_process";
dotenv.config();
const timeout = 60000;

const api_key = process.env.AMP_API_KEY;
const api_secret = process.env.AMP_API_SECRET;

const CONFIG = {
	api_key,
	api_secret,
	start_date: "2021-09-17",
	end_date: "2021-09-29",
	region: 'US',
	time_unit: 'day',
	tempDir: "./tmp",
	verbose: false,
	cleanup: true
};



describe('do tests work?', () => {
	test('a = a', () => {
		expect(true).toBe(true);
	});
});


describe('e2e', () => {
	test('works as module', async () => {
		console.log('MODULE TEST');
		const results = await main(CONFIG);
		const expected = ["/Users/ak/code/amp-ext/exports/2021-09-17_17#0.json", "/Users/ak/code/amp-ext/exports/2021-09-21_16#0.json", "/Users/ak/code/amp-ext/exports/2021-09-26_18#0.json", "/Users/ak/code/amp-ext/exports/2021-09-27_23#0.json"];
		expect(results).toEqual(expected);
	}, timeout);

	test('works as CLI', async () => {
		console.log('CLI TEST');
		const {
			api_key,
			api_secret,
			start_date,
			end_date,
			region,
			time_unit,
			tempDir,
			verbose,
			cleanup } = CONFIG;
		const run = execSync(`node ./index.js --key ${api_key} --secret ${api_secret} --start ${start_date} --end ${end_date} --region ${region} --unit ${time_unit} --tempDir ${tempDir} --cleanup ${cleanup} --verbose ${verbose}`);
		expect(run.toString().trim().includes('hooray! all done!')).toBe(true);
	});
});



afterEach(() => {

	// console.log('TEST FINISHED deleting entities...');
	// execSync(`npm run delete`);
	// console.log('...entities deleted 👍');
	console.log('clearing...');
	execSync(`npm run prune`);
	console.log('...files cleared 👍');
});

afterAll(() => {

	// console.log('TEST FINISHED deleting entities...');
	// execSync(`npm run delete`);
	// console.log('...entities deleted 👍');
	console.log('clearing...');
	execSync(`npm run prune`);
	console.log('...files cleared 👍');
});