import React from 'react';
import fetch from 'node-fetch';

class Field extends React.Component
{
	constructor(props)
	{
		super(props);
		this.state = {
			data: {
				width: 0,
				height: 0,
				movesPerTurn: 0,
				events: [],
			},
			occupants: [],
		};
		this.rowRenderer = this.rowRenderer.bind(this);
		this.occupantRenderer = this.occupantRenderer.bind(this);
		fetch(this.props.gameUrl)
			.then(response => response.json())
			.then(responseJson => {
				this.setState(this.calcState(responseJson));
			});
	}

	calcState(data)
	{
		const newState = {
			data,
			positionIx: 0,
			occupants: Array.from(
				{length: data.height},
				(elem, ix) => Array.from(
					{length: data.width},
					(elem, ix) => ({
						player: null,
						stage: null,
					}),
				)
			),
			playerCssMapping: {},
		};
		const playerAliases = 'abcd';
		var playerAliasIx = -1;
		var stop = false;
		for(const event of data.events)
		{
			switch(event.stage)
			{
				case 'Home':
				newState.playerCssMapping[event.player] = playerAliases[++playerAliasIx];
				break;

				case 'Outpost':
				case 'Block':
				stop = true;
				break;
			}
			if(stop)
			{
				break;
			}
		}
		return newState;
	}

	getMoveDetails(move)
	{
		if(!move)
			return 'N/A';
		return move.stage + ' ' + move.player;
	}

	advance(positionDelta)
	{
		const newState = {
			data: {
				...this.state.data,
			},
			occupants: [...this.state.occupants],
			positionIx: this.state.positionIx + positionDelta,
		};
		const dIx = positionDelta > 0 ? 1 : -1;
		for(var ix = this.state.positionIx; ix !== newState.positionIx; ix += dIx)
		{
			const event = dIx > 0 ? newState.data.events[ix] : newState.data.events[ix + dIx];
			switch(event.stage)
			{
				case 'Home':
				var x, y;
				const [fullMatch, verticalAlignment, horizontalAlignment] = /(Top|Bottom)(Left|Right)/.exec(event.quadrant);
				switch(verticalAlignment)
				{
					case 'Top':
					y = 0;
					break;

					case 'Bottom':
					y = this.state.data.height - 1;
					break;
				}
				switch(horizontalAlignment)
				{
					case 'Left':
					x = 0;
					break;

					case 'Right':
					x = this.state.data.width - 1;
					break;
				}
				if(dIx < 0)
				{
					newState.occupants[y][x] = {
						player: null,
						stage: null,
					};
				}
				else
				{
					newState.occupants[y][x] = {
						player: event.player,
						stage: event.stage,
					}
				}
				break;

				case 'Outpost':
				case 'Block':
				if(dIx < 0)
					newState.occupants[event.y][event.x] = event.prevOccupant;
				else
				{
					newState.data.events[ix]['prevOccupant'] = this.state.occupants[event.y][event.x];
					newState.occupants[event.y][event.x] = {
						player: event.player,
						stage: event.stage,
					};
				}
				break;
			}
			}
		this.setState(newState);
	}

	advanceTo(positionIx)
	{
		this.advance(positionIx - this.state.positionIx);
	}

	occupantRenderer(occupant) {
		if(!occupant)
			return <td><div className="gameCell"></div></td>;
		return <td>
			<div className={`gameCell ${occupant.stage} ${this.state.playerCssMapping[occupant.player]}-${occupant.stage}`}>
			{ {
				Home: '\u2665',
				Outpost: '*',
				Block: '',
				null: '',
			}[occupant.stage] }
			</div>
		</td>;
	}

	rowRenderer(rowElem) {
		return <tr>
			{ rowElem.map(this.occupantRenderer) }
		</tr>;
	}

	render()
	{
		return <div>
			<div>
				<button onClick={event => this.advanceTo(0)}>|&lt;&lt;</button>
				<button onClick={event => this.advance(-1)}>&lt;</button>
				<button onClick={event => this.advance(1)}>&gt;</button>
				<button onClick={event => this.advanceTo(this.state.data.events.length)}>&gt;&gt;|</button>
				<input type="range" min="0" max={this.state.data.events.length} value={this.state.positionIx} onInput={event => this.advanceTo(event.target.value)} />
			</div>
			<table className="cellGrid">
				{ this.state.occupants.map(this.rowRenderer) }
			</table>
		</div>;
	}
}

export default Field;
