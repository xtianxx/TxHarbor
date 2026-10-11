// SPDX-License-Identifier: MIT
// TxHarbor portfolio demo fixture - NOT FOR PRODUCTION.
//
// Minimal ERC-20 used only as a local Anvil test asset for the portfolio demo:
// the full common interface (totalSupply/balanceOf/transfer/allowance/approve/
// transferFrom) plus the Transfer/Approval events. The constructor mints a
// fixed one-time supply to the deployer; there is no mint, owner or admin path.
pragma solidity 0.8.30;

contract MinimalERC20 {
    string public constant name = "TxHarbor Demo Token";
    string public constant symbol = "TXHD";
    uint8 public constant decimals = 18;

    /// @dev Fixed one-time supply (1,000,000 * 10^18), minted in the constructor.
    uint256 public constant INITIAL_SUPPLY = 1_000_000 * 10 ** 18;

    uint256 private _totalSupply;
    mapping(address => uint256) private _balances;
    mapping(address => mapping(address => uint256)) private _allowances;

    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);

    constructor() {
        _totalSupply = INITIAL_SUPPLY;
        _balances[msg.sender] = INITIAL_SUPPLY;
        emit Transfer(address(0), msg.sender, INITIAL_SUPPLY);
    }

    function totalSupply() external view returns (uint256) {
        return _totalSupply;
    }

    function balanceOf(address account) external view returns (uint256) {
        return _balances[account];
    }

    function allowance(address owner, address spender) external view returns (uint256) {
        return _allowances[owner][spender];
    }

    function transfer(address to, uint256 value) external returns (bool) {
        _transfer(msg.sender, to, value);
        return true;
    }

    function approve(address spender, uint256 value) external returns (bool) {
        _allowances[msg.sender][spender] = value;
        emit Approval(msg.sender, spender, value);
        return true;
    }

    function transferFrom(address from, address to, uint256 value) external returns (bool) {
        uint256 allowed = _allowances[from][msg.sender];
        require(allowed >= value, "MinimalERC20: allowance exceeded");
        if (allowed != type(uint256).max) {
            _allowances[from][msg.sender] = allowed - value;
        }
        _transfer(from, to, value);
        return true;
    }

    function _transfer(address from, address to, uint256 value) private {
        require(to != address(0), "MinimalERC20: transfer to zero");
        uint256 fromBalance = _balances[from];
        require(fromBalance >= value, "MinimalERC20: balance exceeded");
        unchecked {
            _balances[from] = fromBalance - value;
        }
        _balances[to] += value;
        emit Transfer(from, to, value);
    }
}
