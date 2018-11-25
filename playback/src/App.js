import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';
import Field from './Field';

class App extends Component {
	constructor(props)
	{
		super(props);
		this.calcQueryParams();
	}

	calcQueryParams()
	{
		this.queryParams = {};
		const query = window.location.search;
		const keyValPat = /[\?\&]([^=]+)=([^\&]+)/g;
		var match;
		while(match = keyValPat.exec(query))
		{
			const key = match[1];
			const value = match[2];
			if(!(key in this.queryParams))
			{
				this.queryParams[key] = [];
			}
			this.queryParams[key].push(value);
		}
	}

	render() 
	{
		return (
			<div className="App">
			<Field gameUrl={this.queryParams.gameUrl[0]} />
			</div>
		);
	}
}

export default App;
